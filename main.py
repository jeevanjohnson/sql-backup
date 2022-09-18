#!/usr/bin/env python
import asyncio
import atexit
import logging
import os
import time
from datetime import datetime

from aiobotocore.session import get_session
from dotenv import load_dotenv

# set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

load_dotenv(dotenv_path=".env")

LOG_LEVEL = os.getenv("LOG_LEVEL")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_BUCKET_REGION = os.getenv("AWS_BUCKET_REGION")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")

assert LOG_LEVEL is not None
assert DB_USER is not None
assert DB_PASS is not None
assert DB_NAME is not None
assert AWS_ACCESS_KEY_ID is not None
assert AWS_SECRET_ACCESS_KEY is not None
assert AWS_BUCKET_NAME is not None
assert AWS_BUCKET_REGION is not None
assert AWS_ENDPOINT_URL is not None


def magnitude_format_size(size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]:
        if size < 1024.0:
            return f"{size:3.1f} {unit}"
        size /= 1024.0
    raise NotImplementedError("Size too large")


def magnitude_time_format(seconds: float) -> str:
    for unit in ["s", "m", "h"]:
        if seconds < 60.0:
            return f"{seconds:.2f}{unit}"
        seconds /= 60.0
    raise NotImplementedError("Time too large")


def remove_if_exists(filename: str) -> None:
    if os.path.exists(filename):
        os.remove(filename)


async def main() -> int:
    start_time = time.perf_counter()
    backup_filename = f"backup_{datetime.now().isoformat()}.sql"

    # make sure the backup is removed from disk
    atexit.register(remove_if_exists, backup_filename)

    with open(backup_filename, "wb+") as backup_file:
        process = await asyncio.subprocess.create_subprocess_shell(
        f"sudo mysqldump -u {DB_USER} -p{DB_PASS} {DB_NAME}",
        stdout=backup_file,
        stderr=asyncio.subprocess.PIPE,
    )
        _, stderr = await process.communicate()

    exit_code = process.returncode
    assert exit_code is not None
    if exit_code != 0:
        if process.stderr is not None:
            logging.error(stderr.decode())
        return exit_code

    try:
        session = get_session()  # TODO: env vars?
        with open(backup_filename, "rb") as backup_file:
            async with session.create_client(
                service_name="s3",
                region_name=AWS_BUCKET_REGION,
                endpoint_url=AWS_ENDPOINT_URL,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            ) as s3_client:
                await s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=f"db-backups/{backup_filename}",
                    Body=backup_file,
                )
    except Exception as e:
        logging.error(f"{backup_filename} failed uploading to bucket")
        logging.error(e)
        return 1

    # get file stats for backup file on disk
    stat_result = os.stat(backup_filename)

    file_size = magnitude_format_size(stat_result.st_size)
    time_elapsed = magnitude_time_format(time.perf_counter() - start_time)
    logging.info(f"{backup_filename} ({file_size}) uploaded in {time_elapsed}")

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)

    exit_code = asyncio.run(main())
    raise SystemExit(exit_code)
