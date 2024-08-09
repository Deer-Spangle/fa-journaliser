import dataclasses
import datetime
from functools import lru_cache

from bs4 import BeautifulSoup

@dataclasses.dataclass
class JournalInfo:
    soup: BeautifulSoup

    @classmethod
    def from_content(cls, content: str) -> "JournalInfo":
        print(f"Start of content: {content[:50]}")
        soup = BeautifulSoup(content, "html.parser")
        return JournalInfo(soup)

    @lru_cache
    def page_title(self) -> str:
        return self.soup.select_one("title").string

    @lru_cache
    def is_system_error(self) -> bool:
        return self.page_title() == "System Error"

    @lru_cache
    def journal_exists(self) -> bool:
        if self.is_system_error():
            return "you are trying to find is not in our database." in self.error_message()
        return False

    @lru_cache
    def error_message(self) -> str:
        return self.soup.select_one("table.maintable td.alt1 font").string

    @lru_cache
    def posted_at(self) -> datetime.datetime:
        date_elem = self.soup.select_one(".journal-title-box .popup_date")

"""
    def stuff(self) -> str:
        date = pick_date(html.at_css(".journal-title-box .popup_date"))
        tag.content.include?("ago") ? tag["title"]: tag.content

        profile_url = html.at_css("td.cat .journal-title-box a")["href"][1..-1]
        journal_header =
          unless html.at_css(".journal-header").nil?
            html.at_css(".journal-header").children[0..-3].to_s.strip
          end
        journal_footer =
          unless html.at_css(".journal-footer").nil?
            html.at_css(".journal-footer").children[2..-1].to_s.strip
          end

        {
          title: html.at_css(".journal-title-box .no_overflow").content.gsub(/\A[[:space:]]+|[[:space:]]+\z/, ""),
          description: html.at_css("td.alt1 div.no_overflow").children.to_s.strip,
          journal_header: journal_header,
          journal_body: html.at_css(".journal-body").children.to_s.strip,
          journal_footer: journal_footer,
          name: html.at_css("td.cat .journal-title-box a").content,
          profile: fa_url(profile_url),
          profile_name: last_path(profile_url),
          avatar: "https:#{html.at_css("img.avatar")["src"]}",
          link: fa_url("journal/#{id}/"),
          posted: date,
          posted_at: to_iso8601(date)
        }
"""