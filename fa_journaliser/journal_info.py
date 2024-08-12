import dataclasses
import datetime
import re
from functools import cached_property
from typing import Optional

import dateutil.parser
import bs4


class JournalNotFound(Exception):
    pass


class FASystemError(Exception):
    pass


class RegisteredUsersOnly(Exception):
    pass


class AccountDisabled(Exception):
    pass


class PendingDeletion(Exception):
    pass


class DataIncomplete(Exception):
    pass


@dataclasses.dataclass
class JournalInfo:
    journal_id: int
    soup: bs4.BeautifulSoup
    raw_content: str

    @classmethod
    def from_content_bytes(cls, journal_id: int, content: bytes) -> "JournalInfo":
        return cls.from_content(journal_id, content.decode("utf-8", "backslashreplace"))

    @classmethod
    def from_content(cls, journal_id: int, content: str) -> "JournalInfo":
        soup = bs4.BeautifulSoup(content, "html.parser")
        return JournalInfo(journal_id, soup, content)

    @cached_property
    def page_title(self) -> str:
        return self.soup.select_one("title").string

    def check_errors(self) -> None:
        if "</html>" not in self.raw_content:
            raise DataIncomplete()
        if self.journal_deleted:
            raise JournalNotFound()
        if self.is_system_error:
            raise FASystemError(f"System error: {self.error_message}")
        if self.account_private:
            raise RegisteredUsersOnly()
        if self.account_disabled_username:
            raise AccountDisabled(f"Account disabled: {self.account_disabled_username}")
        if self.pending_deletion_by:
            raise PendingDeletion(f"Pending deletion from {self.pending_deletion_by}")

    @cached_property
    def is_system_error(self) -> bool:
        return self.page_title == "System Error"

    @cached_property
    def journal_deleted(self) -> bool:
        if self.is_system_error:
            return "The journal you are trying to find is not in our database." in self.error_message
        return False

    @cached_property
    def account_private(self) -> bool:
        if self.site_content is None:
            return False
        notice_message = self.site_content.select_one("section.notice-message")
        if notice_message is None:
            return False
        redirect = notice_message.select_one(".redirect-message")
        if redirect is None:
            return False
        return "The owner of this page has elected to make it available to registered users only." in redirect.strings

    @cached_property
    def account_disabled_username(self) -> Optional[str]:
        """
        If the page says the account is disabled, return the username of the disabled account. Otherwise return None.
        """
        notice_message = self.site_content.select_one("section.notice-message")
        if notice_message is None:
            return None
        redirect = notice_message.select_one(".redirect-message")
        if redirect is None:
            return None
        disabled_regex = re.compile(
            r'User "([^"]+)" has voluntarily disabled access to their account and all of its contents.'
        )
        for redirect_string in redirect.stripped_strings:
            match = disabled_regex.match(redirect_string)
            if match is not None:
                return match.group(1)
        return None

    @cached_property
    def pending_deletion_by(self) -> Optional[str]:
        notice_message = self.site_content.select_one("section.notice-message")
        if notice_message is None:
            return None
        redirect = notice_message.select_one("p.link-override")
        if redirect is None:
            return None
        deletion_msg = "The page you are trying to reach is currently pending deletion by a request from its owner."
        if deletion_msg in redirect.stripped_strings:
            return "its owner"
        deletion_msg = "The page you are trying to reach is currently pending deletion by a request from the administration."
        if deletion_msg in redirect.stripped_strings:
            return "the administration"
        return None

    @cached_property
    def error_message(self) -> Optional[str]:
        error_elem = self.soup.select_one(".section-body")
        if error_elem:
            return " ".join(error_elem.stripped_strings)
        return None

    @cached_property
    def login_user(self) -> Optional[str]:
        if self.soup.select_one("form.logout-link") is None:
            return None
        avatar = self.soup.select_one("img.loggedin_user_avatar")
        return avatar.attrs["alt"]

    @cached_property
    def site_content(self) -> bs4.element.Tag:
        return self.soup.select_one("#site-content")

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
            "posted_at": self.posted_at.isoformat(),
        }
