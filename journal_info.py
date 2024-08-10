import dataclasses
import datetime
from functools import cached_property
from typing import Optional

import dateutil.parser
import bs4


@dataclasses.dataclass
class JournalInfo:
    journal_id: int
    soup: bs4.BeautifulSoup

    @classmethod
    def from_content(cls, journal_id: int, content: str) -> "JournalInfo":
        print(f"Start of content: {content[:50]}")
        soup = bs4.BeautifulSoup(content, "html.parser")
        return JournalInfo(journal_id, soup)

    @cached_property
    def page_title(self) -> str:
        return self.soup.select_one("title").string

    @cached_property
    def is_system_error(self) -> bool:
        return self.page_title == "System Error"

    @cached_property
    def journal_exists(self) -> bool:
        if self.is_system_error:
            return "you are trying to find is not in our database." in self.error_message
        return False

    @cached_property
    def error_message(self) -> Optional[str]:
        # TODO: this will be wrong
        error_elem = self.soup.select_one("table.maintable td.alt1 font")
        if error_elem:
            return error_elem.string
        return None

    @cached_property
    def content(self) -> bs4.element.Tag:
        return self.soup.select_one(".content")

    @cached_property
    def title(self) -> str:
        return self.content.select_one(".journal-title").string

    @cached_property
    def posted_at(self) -> datetime.datetime:
        date_elem = self.content.select_one("span.popup_date")
        if "ago" in date_elem.string:
            return dateutil.parser.parse(date_elem.attrs["title"])
        return dateutil.parser.parse(date_elem.string)

    def to_json(self) -> dict:
        return {
            "journal_id": self.journal_id,
            "title": self.title,
            # "description": html.at_css("td.alt1 div.no_overflow").children.to_s.strip,
            # "journal_header": journal_header,
            # "journal_body": html.at_css(".journal-body").children.to_s.strip,
            # "journal_footer": journal_footer,
            # "name": html.at_css("td.cat .journal-title-box a").content,
            # "profile": fa_url(profile_url),
            # "profile_name": last_path(profile_url),
            # "avatar": "https:#{html.at_css("img.avatar")["src"]}",
            "link": f"https://furaffinity.net/journal/{self.journal_id}/",
            "posted_at": self.posted_at,
        }
