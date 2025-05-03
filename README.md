# oci_bucket_download_with_PAR_url

A Python script for efficiently downloading objects from Oracle Cloud Infrastructure (OCI) Object Storage using Pre-Authenticated Request (PAR) URLs with asynchronous execution.

## Features

- Asynchronous downloads for improved performance
- Persistent tracking of download status using SQLite database
- Resume capability for interrupted downloads
- Progress tracking with tqdm
- Pagination support for handling large buckets

## Requirements

- Python 3.11+
- Dependencies listed in `requirements.txt`


## Usage

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/oci-par-downloader.git
   cd oci-par-downloader
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
### Basic Usage

```python
from oci_par_downloader import ObjectDownloadPAR

# Initialize the downloader
par_url = "https://objectstorage.region.oraclecloud.com/p/your-par-token/n/namespace/b/bucket-name/o/"
download_dir = "./downloads/"

# Create an instance
downloader = ObjectDownloadPAR(
    oci_par_url=par_url,
    download_dir=download_dir,
    download_timeout=600,  # timeout in seconds
    pagination_limit=100   # objects per page
)

# List and download objects
downloader.list_bucket_objects()  # First list all objects in the bucket
downloader.get_objects()          # Fetch objects for download from the database

# Download objects asynchronously
import asyncio
asyncio.run(downloader.async_download_objects())
```

## How It Works

1. The tool first lists all objects in the bucket using the PAR URL
2. Object metadata is stored in an SQLite database
3. Objects are downloaded asynchronously with progress tracking
4. The tool can resume interrupted downloads by checking the database status

## Advanced Configuration

You can customize the behavior by adjusting these parameters:

- `download_timeout`: HTTP request timeout in seconds
- `pagination_limit`: Number of objects to process in each batch

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.