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

    def __init__(
        self, oci_par_url="", download_dir="", download_timeout=60, pagination_limit=100
    ):

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

    def _setup_db(self):
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
        client = httpx.AsyncClient(timeout=self.timeout)
        try:
            yield client
        finally:
            await client.aclose()

    def __del__(self):
        self.logger.info("Cleaning up resources...")
        try:
            if hasattr(self, "db_connection"):
                self.db_connection.close()
                self.logger.info("SQLite connection closed.")
        except Exception as e:
            self.logger.warning(f"Error closing SQLite connection: {e}")

    def _update_download_status(self, obj_name: str):
        query = """
            UPDATE OCIobjects 
            SET download_status = 1 
            WHERE object_name = ?
        """
        try:
            self.cursor.execute(query, (obj_name,))
            self.db_connection.commit()
            self.logger.debug(f"Updated download status in DB: {obj_name}")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error: {str(e)}")

    async def _async_download_object(self, object):
        object_name, object_url, _ = object
        self.logger.info(f"Starting download: {object_name}")

        local_path = os.path.join(self.download_dir, object_name)

        if os.path.isfile(local_path):
            self._update_download_status(object_name)

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
                    exit()
                    return False
            else:
                self.logger.info(f"Skipped: {object_name} is a directory")
                self._update_download_status(object_name)
                return True

    def list_bucket_objects(self, next_starts_with: str = ""):
        """
        List all objects in the bucket using the PAR URL
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

    def get_objects(self):
        self.logger.info("Fetching objects from DB for download...")
        query = """
            SELECT * FROM OCIobjects
            WHERE download_status=FALSE
            LIMIT ? OFFSET 0
        """

        self.cursor.execute(query, (self.pagination_limit,))
        self.object_list = self.cursor.fetchall()
        self.logger.info(f"Fetched {len(self.object_list)} objects for download.")

    async def async_download_objects(self):
        self.logger.info("Starting batch download...")
        tasks = []
        total = len(self.object_list)
        progress = tqdm(total=total, desc="Downloading", unit="file")

        async def __add_progress_bar(obj):
            result = await self._async_download_object(obj)
            progress.update(1)
            return result

        async with asyncio.TaskGroup() as tg:
            for obj in self.object_list:
                tasks.append(tg.create_task(__add_progress_bar(obj)))

        progress.close()

        self.logger.info("Batch download completed.")


def main(par_url, download_dir):
    logging.info("Running main download flow...")
    downloader = ObjectDownloadPAR(
        oci_par_url=par_url,
        download_dir=download_dir,
        download_timeout=600000,
        pagination_limit=100,
    )

    downloader.list_bucket_objects()
    downloader.get_objects()

    while downloader.object_list:
        ## Asynchronous download
        asyncio.run(downloader.async_download_objects())

        downloader.get_objects()

    logging.info("Nothing to download. Exiting.")


if __name__ == "__main__":
    test_par_url = "https://objectstorage.ap-mumbai-1.oraclecloud.com/p/U3NLfm3pLE9FVCWsYuCsgPx9hnS_IU-yu0Z_6rDSn9pwHKLKzk13PH5JtQh1Luyv/n/bmehiyq3ncw4/b/test_bulk/o/"

    download_location = "./downloads/"

    main(par_url=test_par_url, download_dir=download_location)
