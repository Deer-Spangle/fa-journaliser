import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from fa_journaliser.database import Database
from fa_journaliser.download import run_download
from fa_journaliser.utils import check_downloads, import_downloads

logger = logging.getLogger(__name__)

START_JOURNAL = 10_923_887


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/fa_search_bot.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)
    # Run the bot
    db = Database()
    asyncio.run(db.start())
    asyncio.run(import_downloads(db))
    # TODO: import downloads
    # TODO: implement download with cookies
    # TODO: fill gaps
    # TODO: async all the file operations
    # TODO: Make work forwards work
    # TODO: Make work backwards try using cookies
    sys.exit(0)
