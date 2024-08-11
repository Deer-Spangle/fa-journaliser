import datetime
from typing import Optional

import aiosqlite
from aiosqlite import Connection


class Database:

    def __init__(self) -> None:
        self.db: Optional[Connection] = None

    async def start(self) -> None:
        self.db = await aiosqlite.connect("journals.db")
        await self.db.execute("""CREATE TABLE IF NOT EXISTS `journals` (
            `journal_id` INT NOT NULL,
            `is_deleted` BOOLEAN NOT NULL,
            `archive_datetime` DATETIME NOT NULL,
            `error` TEXT,
            `login_used` TEXT,
            `json` TEXT,
            PRIMARY KEY (`journal_id`)
        );""")
        await self.db.commit()

    async def add_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str]
    ) -> None:
        await self.db.execute(
            "INSERT INTO journals (journal_id, is_deleted, archive_datetime, error, login_used, json) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(journal_id) DO UPDATE SET "
            "is_deleted = ?, error = ?, login_used = ?, json_data = ?",
            (
                journal_id, is_deleted, archive_date, error, login_used, json_data,
                is_deleted, error, login_used, json_data
            )
        )
        await self.db.commit()
