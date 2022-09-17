#!/usr/bin/env python
import asyncio
import logging
import os
import time
from datetime import datetime

from aiobotocore.session import get_session
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_BUCKET_REGION = os.getenv("AWS_BUCKET_REGION")

AWS_ENDPOINT_URL = f"https://s3.{AWS_BUCKET_REGION}.wasabisys.com"

assert DB_USER
assert DB_PASS
assert DB_NAME
assert AWS_ACCESS_KEY_ID
assert AWS_SECRET_ACCESS_KEY
assert AWS_BUCKET_NAME
assert AWS_BUCKET_REGION


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



async def main() -> int:
    start_time = time.perf_counter()
    backup_filename = f"backup_{datetime.now().isoformat()}.sql"

    with open("backup.sql", "wb+") as backup_file:
        process = await asyncio.subprocess.create_subprocess_shell(
        f"sudo mysqldump -u {DB_USER} -p{DB_PASS} {DB_NAME}",
        stdout=backup_file,
        stderr=asyncio.subprocess.PIPE,
    )
        stdout, stderr = await process.communicate()

    exit_code = process.returncode
    assert exit_code is not None
    if exit_code != 0:
        if process.stderr is not None:
            logging.error(stderr.decode())
        return exit_code

    try:
        session = get_session()  # TODO: env vars?
        with open("backup.sql", "rb") as backup_file:
            async with session.create_client(
                "s3",
                endpoint_url=AWS_ENDPOINT_URL,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            ) as s3_client:
                await s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=backup_filename,
                    Body=backup_file,
                )
    except Exception as e:
        logging.warning(f"{backup_filename} failed uploading to bucket")
        logging.error(e)
        return 1

    file_size = magnitude_format_size(len(stdout))
    time_elapsed = magnitude_time_format(time.perf_counter() - start_time)
    logging.info(f"{backup_filename} ({file_size}) uploaded in {time_elapsed}")

    return 0


if __name__ == "__main__":
    import atexit
    atexit.register(os.remove, "backup.sql")

    exit_code = asyncio.run(main())
    raise SystemExit(exit_code)
