import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from fa_journaliser.download import run_download
from fa_journaliser.utils import check_downloads

logger = logging.getLogger(__name__)

COOKIE_A = ""
COOKIE_B = ""
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
    check_downloads()
    asyncio.run(run_download())
