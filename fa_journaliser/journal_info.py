import dataclasses
import datetime
import re
from functools import cached_property
from typing import Optional, TypeVar, Callable

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


def prefix_to_meaning(prefix: str) -> str:
    return {
        None: None,
        "": "",
        "∞": "Deceased",
        "!": "Suspended",
        "~": "Member",
        "-": "Banned",
        "@": "Staff",
    }[prefix]


T = TypeVar("T")

S = TypeVar("S")


def format_if_not_null(elem: Optional[T], format_func: Callable[[T], S]) -> Optional[S]:
    if elem is None:
        return None
    return format_func(elem)



@dataclasses.dataclass
class SiteStatusInfo:
    def __init__(self, stats_elem: bs4.Tag, footnote_elem: bs4.Tag) -> None:
        self.stats_elem: bs4.Tag = stats_elem
        self.footnote_elem: bs4.Tag = footnote_elem

    @cached_property
    def total_online(self) -> int:
        strings = list(self.stats_elem.stripped_strings)
        if strings[1] != "Users online":
            raise ValueError("Total users online stat is not in the right place")
        return int(strings[0])

    @cached_property
    def guests_online(self) -> int:
        strings = list(self.stats_elem.stripped_strings)
        if strings[3] != "guests":
            raise ValueError("Guests online stat is not in the right place")
        return int(strings[2].removeprefix("—").strip())

    @cached_property
    def registered_online(self) -> int:
        strings = list(self.stats_elem.stripped_strings)
        if strings[5] != "registered":
            raise ValueError("Registered online stat is not in the right place")
        return int(strings[4].removeprefix(",").strip())

    @cached_property
    def other_online(self) -> int:
        strings = list(self.stats_elem.stripped_strings)
        if strings[7] != "other":
            raise ValueError("Other online stat is not in the right place")
        return int(strings[6].removeprefix("and").strip())

    @cached_property
    def server_time_at(self) -> datetime.datetime:
        footnote_str = self.footnote_elem.string.strip().removeprefix("Server Time: ")
        return dateutil.parser.parse(footnote_str)

    def to_dict(self) -> dict:
        return {
            "fa_server_time_at": self.server_time_at.isoformat(),
            "online": {
                "total": self.total_online,
                "guests": self.guests_online,
                "registered": self.registered_online,
                "other": self.other_online,
            }
        }



@dataclasses.dataclass
class BadgeInfo:
    position: str  # before or after
    title: str
    class_type: str
    image_url: str

    def to_dict(self) -> dict:
        return {
            "position": self.position,
            "title": self.title,
            "class_type": self.class_type,
            "image_url": self.image_url,
        }

    @classmethod
    def from_img(cls, img_elem: bs4.element.Tag, position: str) -> "BadgeInfo":
        classes = img_elem["class"]
        classes = [c for c in classes if c != "userIcon"]
        return BadgeInfo(
            position,
            img_elem.attrs["title"],
            classes[0],
            "https://furaffinity.net" + img_elem.attrs["src"],
        )


def parse_badges_from_elem(user_elem: bs4.Tag) -> list[BadgeInfo]:
    badges = []
    # Parse badges before the name
    before_elem = user_elem.select_one("usericon-block-before")
    if before_elem is not None:
        badges.extend([
            BadgeInfo.from_img(img_elem, "before") for img_elem in before_elem.select("img")
        ])
    # Parse badges after the name
    after_elem = user_elem.select_one("usericon-block-after")
    if after_elem is not None:
        badges.extend([
            BadgeInfo.from_img(img_elem, "after") for img_elem in after_elem.select("img")
        ])
    return badges


@dataclasses.dataclass
class AuthorInfo:
    display_name: str
    username: str
    avatar_url: str
    status_prefix: Optional[str]
    status_prefix_meaning: Optional[str]
    badges: list[BadgeInfo]
    user_title: Optional[str]
    registered_at: Optional[datetime.datetime]

    def to_dict(self) -> dict:
        return {
            "display_name": self.display_name,
            "username": self.username,
            "avatar": self.avatar_url,
            "status_prefix": self.status_prefix,
            "status_prefix_meaning": self.status_prefix_meaning,
            "badges": [b.to_dict() for b in self.badges],
            "user_title": self.user_title,
            "registered_at": self.registered_at.isoformat(),
        }


@dataclasses.dataclass
class CommentAuthorInfo:
    display_name: str
    username: str
    avatar_url: str
    status_prefix: str
    status_prefix_meaning: str
    badges: list[BadgeInfo]
    user_title: str

    def to_dict(self) -> dict:
        return {
            "display_name": self.display_name,
            "username": self.username,
            "avatar": self.avatar_url,
            "status_prefix": self.status_prefix,
            "status_prefix_meaning": self.status_prefix_meaning,
            "badges": [b.to_dict() for b in self.badges],
            "user_title": self.user_title,
        }


class CommentInfo:
    def __init__(self, elem: bs4.Tag) -> None:
        self.elem = elem

    @cached_property
    def comment_id(self) -> int:
        comment_link = self.elem.select_one("a.comment_anchor")
        comment_id_attr = comment_link.attrs["id"]
        if not comment_id_attr.startswith("cid:"):
            raise ValueError(f"Invalid comment ID: {comment_id_attr}")
        comment_id = int(comment_id_attr.removeprefix("cid:"))
        return comment_id

    @cached_property
    def parent_id(self) -> Optional[int]:
        anchor_elem = self.elem.select_one("comment-anchor")
        if anchor_elem is None:
            return None
        # The parent link is commented out... why?
        comment_elem: Optional[bs4.Comment] = None
        for child_elem in anchor_elem.children:
            if isinstance(child_elem, bs4.Comment):
                comment_elem = child_elem
        if comment_elem is None:
            return None
        # Now parse the comment as html...
        comment_str = str(comment_elem).strip()
        comment_soup = bs4.BeautifulSoup(comment_str, "html.parser")
        parent_link = comment_soup.select_one("a.comment-parent")
        if parent_link is None:
            return None
        # Now get the parent ID from the link
        parent_href = parent_link.attrs["href"]
        if not parent_href.startswith("#cid:"):
            raise ValueError(f"Invalid parent href: {parent_href}")
        parent_id = int(parent_href.removeprefix("#cid:"))
        return parent_id

    @cached_property
    def deletion_message(self) -> Optional[str]:
        deleted_elem = self.elem.select_one("comment-container.deleted-comment-container")
        if deleted_elem is None:
            return None
        comment_text = deleted_elem.select_one("comment-user-text")
        # Sometimes there is an extra span inside, if it's `[deleted]`
        span_elem = comment_text.select_one("span.block__deleted_content")
        if span_elem is not None:
            return span_elem.string.strip()
        # Sometimes there is not, if it's `Comment hidden by its owner`
        return comment_text.string.strip()

    @cached_property
    def author_avatar(self) -> Optional[str]:
        avatar = self.elem.select_one("img.comment_useravatar")
        if avatar is None:
            return None
        return "https" + avatar.attrs["src"]

    @cached_property
    def author_username(self) -> Optional[str]:
        avatar = self.elem.select_one("img.comment_useravatar")
        if avatar is None:
            return None
        return avatar.attrs["alt"]

    @cached_property
    def author_display_name(self) -> Optional[str]:
        display_name_elem = self.elem.select_one("comment-username strong.comment_username")
        if display_name_elem is None:
            display_name_elem = self.elem.select_one("comment-username .c-usernameBlock__displayName span")
            if display_name_elem is None:
                return None
        return display_name_elem.string.strip()

    @cached_property
    def author_status_prefix(self) -> Optional[str]:
        username_elem = self.elem.select_one(".c-usernameBlock__userName")
        if username_elem is None:
            return None
        prefix_elem = username_elem.select_one(".c-usernameBlock__symbol")
        prefix = "".join(prefix_elem.stripped_strings)
        return prefix

    @cached_property
    def author_status_prefix_meaning(self) -> Optional[str]:
        return prefix_to_meaning(self.author_status_prefix)

    @cached_property
    def author_badges(self) -> Optional[list[BadgeInfo]]:
        username_elem = self.elem.select_one("comment-username")
        badges = parse_badges_from_elem(username_elem)
        badges = [b for b in badges if b.class_type != "edited-icon"]
        return badges

    @cached_property
    def author_title(self) -> Optional[str]:
        title_elem = self.elem.select_one("comment-title")
        return title_elem.string.strip()

    @cached_property
    def author(self) -> Optional[CommentAuthorInfo]:
        if self.deletion_message is not None:
            return None
        return CommentAuthorInfo(
            self.author_display_name,
            self.author_username,
            self.author_avatar,
            self.author_status_prefix,
            self.author_status_prefix_meaning,
            self.author_badges,
            self.author_title,
        )

    @cached_property
    def posted_at(self) -> Optional[datetime.datetime]:
        if self.deletion_message is not None:
            return None
        date_elem = self.elem.select_one("comment-date span.popup_date")
        if "ago" in date_elem.string:
            return dateutil.parser.parse(date_elem.attrs["title"])
        return dateutil.parser.parse(date_elem.string)

    @cached_property
    def comment_body(self) -> Optional[str]:
        body_elem = self.elem.select_one("comment-user-text .user-submitted-links")
        if body_elem is None:
            return None
        return body_elem.decode_contents().strip()

    @cached_property
    def is_op(self) -> bool:
        op_elem = self.elem.select_one("comment-username span.comment_op_marker")
        if op_elem is None:
            return False
        return True

    @cached_property
    def edited(self) -> bool:
        if self.deletion_message is not None:
            return False
        username_elem = self.elem.select_one("comment-username")
        badges = parse_badges_from_elem(username_elem)
        if "edited-icon" in [b.class_type for b in badges]:
            return True
        return False

    def to_dict(self) -> dict:
        return {
            "comment_id": self.comment_id,
            "parent_id": self.parent_id,
            "deletion_message": self.deletion_message,
            "author": format_if_not_null(self.author, lambda a: a.to_dict()),
            "posted_at": format_if_not_null(self.posted_at, lambda d: d.isoformat()),
            "comment_body": self.comment_body,
            "is_op": self.is_op,
            "edited": self.edited,
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
        if self.is_data_incomplete:
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
    def is_data_incomplete(self) -> bool:
        return "</html>" not in self.raw_content

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
        if any([
                "has voluntarily disabled access to their account and all of its contents." in redirect.stripped_strings,
                "Access has been disabled to the account and contents of user" in redirect.stripped_strings,
        ]):
            user_link_elem = redirect.select_one(".c-usernameBlockSimple a")
            user_page_link = user_link_elem["href"]
            username = user_page_link.removesuffix("/").removeprefix("/user/")
            return username
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
        username_elem = self.userpage_nav_header.select_one(".c-usernameBlock__displayName")
        if username_elem is not None:
            display_name = "".join(username_elem.stripped_strings)
            return display_name
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
        username_elem = self.userpage_nav_header.select_one(".c-usernameBlock__userName")
        if username_elem is not None:
            prefix_elem = username_elem.select_one(".c-usernameBlock__symbol")
            prefix = "".join(prefix_elem.stripped_strings)
            return prefix
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
        return prefix_to_meaning(self.author_status_prefix)

    @cached_property
    def author_badges(self) -> Optional[list[BadgeInfo]]:
        if self.userpage_nav_header is None:
            return None
        username_elem = self.userpage_nav_header.select_one("username")
        return parse_badges_from_elem(username_elem)

    @cached_property
    def _author_user_title_elems(self) -> Optional[list[bs4.PageElement]]:
        if self.userpage_nav_header is None:
            return None
        user_title_elem = self.userpage_nav_header.select_one(".user-title")
        if user_title_elem is None:
            return None
        title_elems = user_title_elem.contents
        if len(title_elems) != 3:
            raise ValueError(f"Could not parse user-title, element does not have 3 children: {title_elems}")
        return title_elems

    @cached_property
    def author_title(self) -> Optional[str]:
        title_elems = self._author_user_title_elems
        if title_elems is None:
            return None
        user_title = str(title_elems[0]).strip().removesuffix("|").rstrip()
        return user_title

    @cached_property
    def author_registered_at(self) -> Optional[datetime.datetime]:
        title_elems = self._author_user_title_elems
        if title_elems is None:
            return None
        registered_str = str(title_elems[-1]).strip()
        return dateutil.parser.parse(registered_str)

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
    def author(self) -> Optional[AuthorInfo]:
        if self.userpage_nav_header is None:
            return None
        return AuthorInfo(
            self.author_display_name,
            self.author_username,
            self.author_avatar,
            self.author_status_prefix,
            self.author_status_prefix_meaning,
            self.author_badges,
            self.author_title,
            self.author_registered_at,
        )

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

    @cached_property
    def comments(self) -> Optional[list[CommentInfo]]:
        comments_elem = self.soup.select_one("#comments-journal")
        if comments_elem is None:
            return None
        comments: list[CommentInfo] = []
        for comment_elem in comments_elem.select(".comment_container"):
            comments.append(CommentInfo(comment_elem))
        return comments

    @cached_property
    def num_comments(self) -> int:
        if self.comments is None:
            return 0
        return len(self.comments)

    @cached_property
    def latest_comment_posted_at(self) -> Optional[datetime.datetime]:
        latest_datetime = None
        if self.comments is not None:
            for comment in self.comments:
                comment_date = comment.posted_at
                if comment_date is None:
                    continue
                if latest_datetime is None:
                    latest_datetime = comment_date
                    continue
                latest_datetime = max(latest_datetime, comment_date)
        return latest_datetime

    @cached_property
    def site_status(self) -> Optional[SiteStatusInfo]:
        stats_elem = self.soup.select_one(".online-stats")
        footnote_elem = self.soup.select_one(".footnote")
        if stats_elem is None or footnote_elem is None:
            return None
        return SiteStatusInfo(stats_elem, footnote_elem)

    def to_json(self) -> dict:
        return {
            "journal_id": self.journal_id,
            "title": self.title,
            "journal_header": self.journal_header,
            "journal_body": self.journal_content,
            "journal_footer": self.journal_footer,
            "author": format_if_not_null(self.author, lambda a: a.to_dict()),
            "comments_disabled": self.comments_disabled,
            "comments": format_if_not_null(self.comments, lambda comments: [c.to_dict() for c in comments]),
            "num_comments": self.num_comments,
            "latest_comment_posted_at": format_if_not_null(self.latest_comment_posted_at, lambda d: d.isoformat()),
            "link": f"https://furaffinity.net/journal/{self.journal_id}/",
            "posted_at": self.posted_at.isoformat(),
            "site_status": self.site_status.to_dict(),
        }
