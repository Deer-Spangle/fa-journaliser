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


def display_name_to_username(name: str) -> str:
    return name.lower().replace("_", "")


@dataclasses.dataclass
class BadgeInfo:
    position: str  # before or after
    title: str
    class_type: str
    image_url: str

    def to_dict(self):
        return {
            "position": self.position,
            "title": self.title,
            "class_type": self.class_type,
            "image_url": self.image_url,
        }


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
        deletion_msg = (
            "The page you are trying to reach is currently pending deletion by a request from the administration."
        )
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

    @cached_property
    def journal_header(self) -> Optional[str]:
        header_elem = self.content.select_one(".journal-header")
        if header_elem is None:
            return None
        return header_elem.decode_contents().strip()

    @cached_property
    def journal_content(self) -> str:
        content_elem = self.content.select_one(".journal-content")
        return content_elem.decode_contents().strip()

    @cached_property
    def journal_footer(self) -> Optional[str]:
        footer_elem = self.content.select_one(".journal-footer")
        if footer_elem is None:
            return None
        return footer_elem.decode_contents().strip()

    @cached_property
    def userpage_nav_header(self) -> Optional[bs4.element.Tag]:
        if self.site_content is None:
            return None
        return self.site_content.select_one("userpage-nav-header")

    @cached_property
    def author_display_name(self) -> Optional[str]:
        if self.userpage_nav_header is None:
            return None
        username_elem = self.userpage_nav_header.select_one("username")
        display_name = "".join(username_elem.stripped_strings)
        prefix = self.author_status_prefix
        if prefix is not None:
            display_name = display_name.removeprefix(prefix)
        return display_name

    @cached_property
    def author_status_prefix(self) -> Optional[str]:
        if self.userpage_nav_header is None:
            return None
        username_elem = self.userpage_nav_header.select_one("username")
        display_name = "".join(username_elem.stripped_strings)
        potential_prefix = display_name[0]
        username = self.author_username
        # These characters cannot be in a display name, easy
        if potential_prefix in "∞!":
            return potential_prefix
        # Otherwise, try and convert display name and see if they match
        potential_username = display_name_to_username(display_name)
        if username == potential_username:
            return ""
        # If the first letter is another known prefix, and the rest matches the username, found prefix
        # We have to check startswith, because display names can get truncated, see `lapisgamerfoxviewingartsonly`
        if potential_prefix in "~-" and username.startswith(potential_username[1:]):
            return potential_prefix
        # Otherwise, oh no
        raise ValueError(
            f"Unrecognised status prefix. Username was {username}, but display name suggested "
            f"it should be {potential_username}"
        )

    @cached_property
    def author_status_prefix_meaning(self) -> Optional[str]:
        return {
            None: None,
            "": "",
            "∞": "Deceased",
            "!": "Suspended",
            "~": "Member",
            "-": "Banned",
        }[self.author_status_prefix]

    @cached_property
    def author_badges(self) -> Optional[list[BadgeInfo]]:
        if self.userpage_nav_header is None:
            return None
        username_elem = self.userpage_nav_header.select_one("username")
        badges = []
        # Parse badges before the name
        before_elem = username_elem.select_one("usericon-block-before")
        if before_elem is not None:
            for badge_elem in before_elem.select("img"):
                classes = badge_elem["class"]
                classes.remove("userIcon")
                badges.append(BadgeInfo(
                    "before",
                    badge_elem.attrs["title"],
                    classes[0],
                    "https://furaffinity.net" + badge_elem.attrs["src"],
                ))
        # Parse badges after the name
        after_elem = username_elem.select_one("usericon-block-after")
        if after_elem is not None:
            for badge_elem in after_elem.select("img"):
                classes = badge_elem["class"]
                classes.remove("userIcon")
                badges.append(BadgeInfo(
                    "after",
                    badge_elem.attrs["title"],
                    classes[0],
                    "https://furaffinity.net" + badge_elem.attrs["src"],
                ))
        # Return the list
        return badges

    @cached_property
    def author_username(self) -> Optional[str]:
        if self.userpage_nav_header is None:
            return None
        return self.userpage_nav_header.select_one("userpage-nav-avatar img").attrs["alt"]

    @cached_property
    def author_avatar(self) -> Optional[str]:
        if self.userpage_nav_header is None:
            return None
        avatar_url = self.userpage_nav_header.select_one("userpage-nav-avatar img").attrs["src"]
        if avatar_url.startswith("//"):
            avatar_url = f"https{avatar_url}"
        return avatar_url

    @cached_property
    def comments_disabled(self) -> Optional[bool]:
        if self.content is None:
            return False
        response_box = self.content.select_one("#responsebox")
        if response_box is None:
            return False
        response_string = response_box.string
        if response_string is None:
            return False
        return response_string.strip() == "Comment posting has been disabled by the journal owner."

    def to_json(self) -> dict:
        return {
            "journal_id": self.journal_id,
            "title": self.title,
            "journal_header": self.journal_header,
            "journal_body": self.journal_content,
            "journal_footer": self.journal_footer,
            "author": {
                "display_name": self.author_display_name,
                "username": self.author_username,
                "avatar": self.author_avatar,
                "status_prefix": self.author_status_prefix,
                "status_prefix_meaning": self.author_status_prefix_meaning,
                "badges": [b.to_dict() for b in self.author_badges],
                # TODO: user title
            },
            "comments_disabled": self.comments_disabled,
            # TODO: "comments": [
            #    "comment_id": 1234,
            #    "parent_id": None,
            #    "deleted": False,
            #    "author": {},
            #    "posted_at": isoformat
            #    "comment_text": {},
            #     "edited": False,
            #  ]
            "link": f"https://furaffinity.net/journal/{self.journal_id}/",
            "posted_at": self.posted_at.isoformat(),
        }
