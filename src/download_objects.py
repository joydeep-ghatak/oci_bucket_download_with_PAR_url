"""
OCI Object Download - A utility for downloading objects from Oracle Cloud Infrastructure Object Storage.

This script provides functionality to download objects from an OCI bucket using a Pre-Authenticated Request (PAR) URL.
It handles pagination, tracks download status in a SQLite database, and provides concurrent downloading capabilities.
"""

import asyncio
import logging
import os
import sqlite3
import traceback
from contextlib import asynccontextmanager

import httpx
from tqdm.asyncio import tqdm

logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ObjectDownloadPAR:
    """
    A class for downloading objects from Oracle Cloud Infrastructure (OCI) Object Storage
    using Pre-Authenticated Request (PAR) URLs.

    This class handles listing objects in a bucket, tracking download status in a SQLite database,
    and downloading objects concurrently using asyncio.

    Attributes:
        par_url (str): The Pre-Authenticated Request URL for the OCI bucket.
        download_dir (str): Directory where downloaded objects will be stored.
        bucket_name (str): Name of the OCI bucket extracted from the PAR URL.
        object_db (str): SQLite database filename derived from the bucket name.
        object_list (list): List of objects to download, populated from the database.
        object_count (int): Count of objects listed from the bucket.
        timeout (int): HTTP timeout for download operations in seconds.
        pagination_limit (int): Number of objects to process in each pagination cycle.
        logger (Logger): Logger instance for tracking operations.
    """

    def __init__(
        self,
        oci_par_url: str,
        download_dir: str,
        download_timeout: int = 60,
        pagination_limit: int = 100,
    ):
        """
        Initialize the ObjectDownloadPAR instance.

        Args:
            oci_par_url (str): The Pre-Authenticated Request URL for the OCI bucket.
            download_dir (str): Directory where downloaded objects will be stored.
            download_timeout (int): HTTP timeout for download operations in seconds.
            pagination_limit (int): Number of objects to process in each pagination cycle.
        """
        self.par_url = oci_par_url
        self.download_dir = download_dir
        self.bucket_name = self.par_url.split("/b/")[-1].split("/")[0]
        self.object_db = self.bucket_name + ".db"
        self.object_list = []
        self.object_count = 0
        self.timeout = download_timeout
        self.pagination_limit = pagination_limit

        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing ObjectDownloadPAR...")
        self._setup_db()

    def __del__(self):
        """
        Cleanup resources when the object is destroyed.

        Ensures that the SQLite database connection is properly closed.
        """
        self.logger.info("Cleaning up resources...")
        try:
            if hasattr(self, "db_connection"):
                self.db_connection.close()
                self.logger.info("SQLite connection closed.")
        except Exception as e:
            self.logger.warning(f"Error closing SQLite connection: {e}")

    def _setup_db(self):
        """
        Set up the SQLite database for tracking object download status.

        Creates a table 'OCIobjects' if it doesn't exist with columns for
        object name, download URL, and download status.
        """
        self.logger.info("Setting up SQLite database...")
        self.db_connection = sqlite3.connect(self.object_db)
        self.cursor = self.db_connection.cursor()

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OCIobjects(
            object_name TEXT PRIMARY KEY,
            download_url TEXT,
            download_status INTEGER DEFAULT 0)
        """
        )

        self.db_connection.commit()
        self.logger.info("Database setup complete.")

    def _add_obj_in_db(self, obj_name: str, obj_url: str, download_status: int = 0):
        """
        Add an object info to the database.

        Args:
            obj_name (str): Name of the object.
            obj_url (str): URL for downloading the object.
            download_status (int): Download status flag (0=not downloaded, 1=downloaded).

        Note:
            Uses INSERT OR IGNORE to prevent duplicate entries.
        """
        query = """
            INSERT OR IGNORE INTO OCIobjects (object_name, download_url, download_status) 
            VALUES (?, ?, ?)
            """
        try:
            self.cursor.execute(query, (obj_name, obj_url, download_status))
            self.logger.debug(f"Added to DB: {obj_name}")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error: {str(e)}")

    @asynccontextmanager
    async def _get_http_client(self):
        """
        Async context manager for creating and managing an HTTP client.

        Yields:
            httpx.AsyncClient: An async HTTP client with the configured timeout.

        Note:
            Ensures proper cleanup of the client after use.
        """
        client = httpx.AsyncClient(timeout=self.timeout)
        try:
            yield client
        finally:
            await client.aclose()

    def _update_download_status(self, obj_name: str, download_status: bool = True):
        """
        Update the download status of an object in the database.

        Args:
            obj_name (str): Name of the object.
            download_status (bool): True if downloaded, False otherwise.
        """
        query = """
            UPDATE OCIobjects 
            SET download_status = ? 
            WHERE object_name = ?
        """
        try:
            status = 1 if download_status else 0
            self.cursor.execute(query, (status, obj_name))
            self.db_connection.commit()
            self.logger.debug(f"Updated download status in DB: {obj_name}")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error: {str(e)}")

    async def _async_download_object(self, object_info: tuple[str, str, int]):
        """
        Download a single object asynchronously.

        Args:
            object_info (tuple): Tuple containing (object_name, object_url, download_status).

        Returns:
            bool: True if download was successful or file already exists, False otherwise.

        Note:
            Creates directory structure if it doesn't exist.
            Updates download status in database upon completion.
        """
        object_name, object_url, _ = object_info
        self.logger.info(f"Starting download: {object_name}")

        local_path = os.path.join(self.download_dir, object_name)

        if os.path.isfile(local_path):
            self._update_download_status(object_name)
            return True

        else:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            if not os.path.isdir(local_path):
                try:
                    async with self._get_http_client() as async_client:
                        response = await async_client.get(object_url)

                    response.raise_for_status()

                    with open(local_path, "wb") as f:
                        f.write(response.content)
                        # f.write(await response.aread())

                    self.logger.info(f"Downloaded: {object_name}")

                    self._update_download_status(object_name)
                    return True

                except Exception as e:
                    self.logger.error(f"Failed to download {object_name}: {str(e)}")
                    traceback.print_exc()
                    return False
            else:
                self.logger.info(f"Skipped: {object_name} is a directory")
                self._update_download_status(object_name)
                return True

    def list_bucket_objects(self, next_starts_with: str = ""):
        """
        List all objects in the bucket using the PAR URL.

        Args:
            next_starts_with (str): Token for pagination, used to get the next set of objects.

        Note:
            This method is recursive and will continue to call itself until all objects are listed.
            Objects are added to the database as they are listed.
        """
        self.logger.info("Listing objects from bucket...")
        try:
            response = httpx.get(
                self.par_url,
                params={"start": next_starts_with, "limit": 1000},
                timeout=30,
            )

            response_data = response.json()

            objects = response_data["objects"]
            next_starts_with = response_data.get("nextStartWith", None)

            for obj in objects:
                object_name = obj["name"]
                download_url = os.path.join(self.par_url, object_name)

                self._add_obj_in_db(object_name, download_url, 0)

            self.db_connection.commit()
            self.object_count += len(objects)

            self.logger.info(f"Stored {self.object_count} objects in DB so far.")

            if next_starts_with:
                self.logger.info(f"Next page detected: {next_starts_with}")
                self.list_bucket_objects(next_starts_with)
            else:
                self.logger.info("No more objects to download.")

        except Exception as e:
            self.logger.error(f"Failed to list objects: {str(e)}")
            traceback.print_exc()

    def get_downloaded_objects_count(self):
        """
        Get the count of objects that have been successfully downloaded.

        Updates the downloaded_object_count attribute with the current count.
        """
        query = """
            SELECT count(*) FROM OCIobjects
            WHERE download_status=1
        """

        self.cursor.execute(query)
        count = self.cursor.fetchone()
        self.downloaded_object_count = count[0] if count else 0
        # self.logger.info(f"Fetched {len(self.downloaded_object_list)} objects which have been downloaded.")

    def get_downloaded_objects(self, offset: int = 0):
        """
        Retrieve a paginated list of downloaded objects from the database.

        Args:
            offset (int): Pagination offset for SQL query.

        Note:
            Results are stored in the downloaded_object_list attribute.
        """
        query = """
            SELECT * FROM OCIobjects
            WHERE download_status=1
            LIMIT ? OFFSET ?
        """

        self.cursor.execute(query, (self.pagination_limit, offset))
        self.downloaded_object_list = self.cursor.fetchall()
        self.logger.info(
            f"Fetched {len(self.downloaded_object_list)} objects which have been downloaded."
        )

    def get_objects(self):
        """
        Retrieve a paginated list of objects that need to be downloaded.

        Note:
            Results are stored in the object_list attribute.
            Only retrieves objects with download_status=0.
        """
        self.logger.info("Fetching objects from DB for download...")
        query = """
            SELECT * FROM OCIobjects
            WHERE download_status=0
            LIMIT ? OFFSET 0
        """

        self.cursor.execute(query, (self.pagination_limit,))
        self.object_list = self.cursor.fetchall()
        self.logger.info(f"Fetched {len(self.object_list)} objects for download.")

    async def check_files_exist(self):
        """
        Verify that files marked as downloaded in the database actually exist on disk.

        If a file doesn't exist, its download status is reset to 0 in the database.

        Note:
            This method processes downloaded objects in batches defined by pagination_limit.
        """
        loop = asyncio.get_running_loop()

        async def file_exists(obj):
            """
            Check if a file exists on disk and update its status if not.

            Args:
                obj (tuple): Tuple containing (object_name, object_url, download_status).
            """
            object_name, _, _ = obj
            # self.logger.info(f"Starting download: {object_name}")

            local_path = os.path.join(self.download_dir, object_name)
            if not await loop.run_in_executor(None, lambda: os.path.isfile(local_path)):
                self._update_download_status(
                    obj_name=object_name, download_status=False
                )

        self.get_downloaded_objects_count()

        checked_count = 0
        self.get_downloaded_objects()
        while self.downloaded_object_list:
            tasks = [file_exists(file) for file in self.downloaded_object_list]
            await asyncio.gather(*tasks)
            checked_count += len(self.downloaded_object_list)
            self.get_downloaded_objects(checked_count)

    async def async_download_objects(self):
        """
        Download multiple objects concurrently using asyncio.

        Uses the object_list attribute to determine which objects to download.
        Displays a progress bar using tqdm to show download progress.

        Note:
            This method should be called after get_objects() has been used to populate object_list.
        """
        self.logger.info("Starting batch download...")
        tasks = []
        total = len(self.object_list)
        progress = tqdm(total=total, desc="Downloading", unit="file")

        async def __add_progress_bar(obj):
            """
            Wrapper function that updates the progress bar after a download completes.

            Args:
                obj (tuple): Tuple containing (object_name, object_url, download_status).

            Returns:
                bool: Result from _async_download_object.
            """
            result = await self._async_download_object(obj)
            progress.update(1)
            return result

        async with asyncio.TaskGroup() as tg:
            for obj in self.object_list:
                tasks.append(tg.create_task(__add_progress_bar(obj)))

        progress.close()

        self.logger.info("Batch download completed.")


def main(par_url: str, download_dir: str):
    """
    Main function to orchestrate the download process.

    Args:
        par_url (str): The Pre-Authenticated Request URL for the OCI bucket.
        download_dir (str): Directory where downloaded objects will be stored.

    Note:
        This function creates a downloader instance, lists objects in the bucket,
        checks for already downloaded files, and downloads any remaining objects.
    """
    logging.info("Running main download flow...")
    downloader = ObjectDownloadPAR(
        oci_par_url=par_url,
        download_dir=download_dir,
        download_timeout=600000,
        pagination_limit=100,
    )

    downloader.list_bucket_objects()

    asyncio.run(downloader.check_files_exist())

    downloader.get_objects()
    while downloader.object_list:
        asyncio.run(downloader.async_download_objects())
        downloader.get_objects()

    logging.info("Nothing to download. Exiting.")


if __name__ == "__main__":
    test_par_url = "https://objectstorage.ap-mumbai-1.oraclecloud.com/p/U3NLfm3pLE9FVCWsYuCsgPx9hnS_IU-yu0Z_6rDSn9pwHKLKzk13PH5JtQh1Luyv/n/bmehiyq3ncw4/b/test_bulk/o/"

    download_location = "./downloads/"

    main(par_url=test_par_url, download_dir=download_location)
