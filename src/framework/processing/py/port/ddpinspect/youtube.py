"""
DDP Youtube module

This module contains functions to handle files contained within an youtube ddp
"""

from pathlib import Path
from typing import Any
import logging
import zipfile
import io
import re

import pandas as pd
from bs4 import BeautifulSoup

from port.ddpinspect import scanfiles
from port.ddpinspect.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)


logger = logging.getLogger(__name__)

VIDEO_REGEX = r"(?P<video_url>^http[s]?://www\.youtube\.com/watch\?v=[a-z,A-Z,0-9,\-,_]+)(?P<rest>$|&.*)"
CHANNEL_REGEX = r"(?P<channel_url>^http[s]?://www\.youtube\.com/channel/[a-z,A-Z,0-9,\-,_]+$)"

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "archive_browser.html",
            "watch-history.json",
            "my-comments.html",
            "my-live-chat-messages.html",
            "subscriptions.csv",
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "archive_browser.html",
            "watch-history.html",
            "my-comments.html",
            "my-live-chat-messages.html",
            "subscriptions.csv",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "archive_browser.html",
            "kijkgeschiedenis.json",
            "mijn-reacties.html",
            "abonnementen.csv",
        ],
    ),
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "archive_browser.html",
            "kijkgeschiedenis.html",
            "mijn-reacties.html",
            "abonnementen.csv",
        ],
    ),
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message="Valid zip"),
    StatusCode(id=1, description="Bad zipfile", message="Bad zipfile"),
]


def validate_zip(zfile: Path) -> ValidateInput:
    """
    Validates the input of a Youtube zipfile
    """

    validate = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json", ".csv"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validate.set_status_code(0)
        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_status_code(1)

    return validate


def to_df(youtube_list: list[dict[Any, Any]] | Any) -> pd.DataFrame:
    """
    Converts list[dict[Any, Any]] obtained from youtube to pd.DataFrame

    I don't know yet whether this function is general enough to be moved to a different module
    For now I like it here
    """

    df_out = pd.DataFrame()

    try:
        if not isinstance(youtube_list, (dict, list)):
            raise TypeError("Incorrect input type expected dict or list")

        df_out = pd.DataFrame([scanfiles.dict_denester(item) for item in youtube_list])
        df_out = scanfiles.remove_const_cols_from_df(df_out)

    except TypeError as e:
        logger.error(e)
    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    finally:
        return df_out


def bytes_to_soup(buf: io.BytesIO) -> BeautifulSoup:
    """
    Remove undecodable bytes from utf-8 string
    BeautifulSoup will hang otherwise
    """
    utf_8_str = buf.getvalue().decode("utf-8", errors="ignore")
    utf_8_str = re.sub(r'[^\x00-\x7F]+', ' ', utf_8_str)
    soup = BeautifulSoup(utf_8_str, "lxml")
    return soup


def comments_to_df(comments: io.BytesIO) -> pd.DataFrame:
    """
    Parse comments from Youtube DDP

    returns a pd.DataFrame
    with the comment, type of comment, and a video url
    """

    data_set = []
    df = pd.DataFrame()
    video_pattern = re.compile(VIDEO_REGEX)

    # Big try except block due to lack of time
    try:
        soup = bytes_to_soup(comments)
        items = soup.find_all("li")
        for item in items:
            data_point = {}

            # Extract comments
            content = item.get_text(separator="<SEP>").split("<SEP>")
            message = content.pop()
            action = "".join(content)
            data_point["Comment"] = message
            data_point["Type of comment"] = action

            # Search through all references
            # if a video can be found:
            # 1. extract video url
            # 2. add data point
            for ref in item.find_all("a"):
                regex_result = video_pattern.match(ref.get("href"))
                if regex_result:
                    data_point["Video url"] = regex_result.group("video_url")
                    data_set.append(data_point)
                    break

        df = pd.DataFrame(data_set)

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    finally:
        return df


def watch_history_html_to_df(watch_history: io.BytesIO) -> pd.DataFrame:
    """
    Extacts the watch history in html format from Youtube DDP

    returns a pd.DataFrame
    """

    df = pd.DataFrame()
    data_set = []
    video_pattern = re.compile(VIDEO_REGEX)
    channel_pattern = re.compile(CHANNEL_REGEX)

    try:
        soup = bytes_to_soup(watch_history)
        watch_item_id = "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
        items = soup.find_all("div", {"class": watch_item_id})
        for item in items:
            data_point = {}

            content = item.get_text(separator="<SEP>").split("<SEP>")
            time = content.pop()
            data_point["Time"] = time

            for ref in item.find_all("a"):
                video_regex_result = video_pattern.match(ref.get("href"))
                channel_regex_result = channel_pattern.match(ref.get("href"))

                if video_regex_result:
                    data_point["Video title"] = ref.get_text()
                    data_point["Video url"] = video_regex_result.group("video_url")

                if channel_regex_result:
                    data_point["Channel title"] = ref.get_text()
                    data_point["Channel url"] = channel_regex_result.group("channel_url")

                if video_regex_result:
                    data_set.append(data_point)

        df = pd.DataFrame(data_set)

        try:
            df = df[['Video title', 'Video url', 'Channel title', 'Channel url', 'Time']]
        except KeyError as e:
            logger.info("Column order could not be changed: %s", e)

    except Exception as e:
        logger.error("Watch history could not be extracted: %s", e)
    finally:
        return df
