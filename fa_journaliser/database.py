import datetime
from typing import Optional

import aiosqlite
import prometheus_client
from aiosqlite import Connection

from fa_journaliser.prom import get_prometheus_port

total_journal_db_entries = prometheus_client.Gauge(
    "fajournaliser_database_journal_entries_total",
    "Total number of journal entries in the database",
)


class Database:

    def __init__(self) -> None:
        self.db: Optional[Connection] = None

    async def start(self) -> None:
        self.db = await aiosqlite.connect("journals.db")
        self.db.row_factory = aiosqlite.Row
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
        # If we're not using prometheus, speed up startup by skipping row count
        if get_prometheus_port() is not None:
            entry_count = await self.count_journals()
            total_journal_db_entries.set(entry_count)

    async def stop(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def count_journals(self) -> int:
        async with self.db.execute("SELECT COUNT(*) AS count FROM journals") as cursor:
            async for row in cursor:
                count = row['count']
        return count

    async def add_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        await self.db.execute(
            "INSERT INTO journals (journal_id, is_deleted, archive_datetime, error, login_used, json) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(journal_id) DO UPDATE SET "
            "is_deleted = ?, error = ?, login_used = ?, json = ?",
            (
                journal_id, is_deleted, archive_date, error, login_used, json_data,
                is_deleted, error, login_used, json_data
            )
        )
        await self.db.commit()
        total_journal_db_entries.inc(1)

    async def update_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        await self.db.execute(
            "UPDATE journals SET "
            "is_deleted = ? AND archive_datetime = ? AND error = ? AND login_used = ? AND json = ? "
            "WHERE journal_id = ?",
            (
                is_deleted, archive_date, error, login_used, json_data, journal_id
            )
        )
        await self.db.commit()

    async def list_journal_ids_truncated(self, min_id: int, max_id: Optional[int]) -> list[int]:
        journal_ids = []
        async with self.db.execute(
                "SELECT journal_id FROM journals WHERE journal_id >= ? AND (? IS NULL OR journal_id <= ?)",
                (min_id, max_id, max_id)
        ) as cursor:
            async for row in cursor:
                journal_ids.append(row['journal_id'])
        return journal_ids

    async def list_ids_where_path_is_null(self, json_path: str) -> list[int]:
        journal_ids = []
        async with self.db.execute(
                "SELECT journal_id FROM journals WHERE error IS NULL AND json_extract(json, ?) IS NULL",
                (json_path,)
        ) as cursor:
            async for row in cursor:
                journal_ids.append(row['journal_id'])
        return journal_ids
