# Fast Python Bulk Loader

Load files from a source folder to a Box folder.  Build to suppport python versions prior to 3.8.x.  Tested with python 3.7.9.  Will run on Windows, Linux and MacOS on Intel.  Will *not* run on Apple Silicon.

## Prerequisites

* Python 3.7.9 
* Properly scoped [Box JWT App Config file](https://developer.box.com/guides/authentication/jwt/jwt-setup/).  App Service Account (or user when `--as-user` is used) nmust have and least Uploader permissions on target folders. 

## Installation Instructions

To be run in installation folder.  

On Windows using Powershell.  For Windows CMD use `.\venv\Scripts\activete.bat` or source `./venv/Scripts/active` on Linux.
```
> puython -m venv .venv
> .\venv\Scripts\activate.ps1
> pip install -U pip setuptools wheel
> pip install -r requirements-37.txt
```

# Usage

```
usage: fast-load.py [-h] [--jwt-config JWT_CONFIG] [--as-user AS_USER]
                    [--workers WORKERS] [--retries RETRIES]
                    [--backoff BACKOFF] [--chunk CHUNK]
                    local_folder box_folder_id

Upload a folder to Box using legacy boxsdk[jwt] with concurrency and retries

positional arguments:
  local_folder
  box_folder_id

optional arguments:
  -h, --help            show this help message and exit
  --jwt-config JWT_CONFIG
                        Path to Box app JWT JSON (or set JWT_CONFIG_BASE_64)
  --as-user AS_USER     Act-as user ID (optional)
  --workers WORKERS
  --retries RETRIES
  --backoff BACKOFF
  --chunk CHUNK         Chunk size for large files

```
