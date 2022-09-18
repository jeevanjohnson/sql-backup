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


async def export_sql_to_file(
    file_name: str,
    db_user: str,
    db_pass: str,
    db_name: str,
) -> int:
    with open(file_name, "wb+") as backup_file:
        process = await asyncio.subprocess.create_subprocess_shell(
        f"sudo mysqldump -u {db_user} -p{db_pass} {db_name}",
        stdout=backup_file,
        stderr=asyncio.subprocess.PIPE,
    )
        # make sure all data is flushed
        _, stderr = await process.communicate()

    if process.stderr is not None:
        logging.error(stderr.decode())

    if process.returncode is None:
        return 1

    return process.returncode


async def upload_file_to_s3(
    file_name: str,
    region_name: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    bucket_name: str,
) -> None:
    session = get_session()  # TODO: env vars?
    with open(file_name, "rb") as backup_file:
        async with session.create_client(
            service_name="s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        ) as s3_client:
            await s3_client.put_object(
                Bucket=bucket_name,
                Key=f"db-backups/{file_name}",
                Body=backup_file,
            )


def remove_if_exists(file_name: str) -> None:
    if os.path.exists(file_name):
        os.remove(file_name)


async def main() -> int:
    start_time = time.perf_counter()
    backup_file_name = f"backup_{datetime.now().isoformat()}.sql"

    # make sure the backup is removed from disk
    atexit.register(remove_if_exists, backup_file_name)

    export_exit_code = await export_sql_to_file(
        db_user=DB_USER,
        db_pass=DB_PASS,
        db_name=DB_NAME,
        file_name=backup_file_name,
    )

    if export_exit_code != 0:
        return export_exit_code

    try:
        await upload_file_to_s3(
            file_name=backup_file_name,
            region_name=AWS_BUCKET_REGION,
            endpoint_url=AWS_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            bucket_name=AWS_BUCKET_NAME,
        )
    except Exception as e:
        logging.error(f"{backup_file_name} failed uploading to bucket")
        logging.error(e)
        return 1

    # get file stats for backup file on disk
    stat_result = os.stat(backup_file_name)

    file_size = magnitude_format_size(stat_result.st_size)
    time_elapsed = magnitude_time_format(time.perf_counter() - start_time)
    logging.info(f"{backup_file_name} ({file_size}) uploaded in {time_elapsed}")

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    raise SystemExit(asyncio.run(main()))
