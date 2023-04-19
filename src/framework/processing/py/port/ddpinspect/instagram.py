"""
DDP Instagram module

This module contains functions to handle *.jons files contained within an instagram ddp
"""

from itertools import product
from typing import Any
from pathlib import Path
import logging
import zipfile
import re

from port.parserlib.stringparse import CannotConvertEpochTimestamp
from port.parserlib import stringparse
from port.ddpinspect import scanfiles
from port.ddpinspect.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "secret_conversations.json",
            "personal_information.json",
            "account_privacy_changes.json",
            "account_based_in.json",
            "recently_deleted_content.json",
            "liked_posts.json",
            "stories.json",
            "profile_photos.json",
            "followers.json",
            "signup_information.json",
            "comments_allowed_from.json",
            "login_activity.json",
            "your_topics.json",
            "camera_information.json",
            "recent_follow_requests.json",
            "devices.json",
            "professional_information.json",
            "follow_requests_you've_received.json",
            "eligibility.json",
            "pending_follow_requests.json",
            "videos_watched.json",
            "ads_interests.json",
            "account_searches.json",
            "following.json",
            "posts_viewed.json",
            "recently_unfollowed_accounts.json",
            "post_comments.json",
            "account_information.json",
            "accounts_you're_not_interested_in.json",
            "use_cross-app_messaging.json",
            "profile_changes.json",
            "reels.json",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message="Valid zip"),
    StatusCode(id=1, description="Bad zipfile", message="Bad zipfile"),
]


def validate_zip(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Instagram zipfile
    """

    validate = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validate.set_status_code(0)
        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_status_code(1)

    return validate


def interests_to_list(dict_with_interests: dict[Any, Any]) -> list[str]:
    """
    This function extracts instagram interests from a dict
    This dict should be obtained from ads_interests.json

    This function should be rewritten as ads_interests.json changes
    """
    out = []

    try:
        if not isinstance(dict_with_interests, dict):
            raise TypeError("The input to this function was not dict")

        # The compleet lookup is:
        # "inferred_data_ig_interest" -> "string_map_data" -> "Interesse"
        # "Interesse is the only key, and the spelling is dutch
        # I suspect this might change with difference language settings
        # Therefore popitem()
        for item in dict_with_interests["inferred_data_ig_interest"]:
            res = item["string_map_data"].popitem()
            out.append(res[1]["value"])

    except TypeError as e:
        logger.error("TypeError: %s", e)
    except KeyError as e:
        logger.error("The a dict did not contain key: %s", e)
    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    finally:
        return out


def your_topics_to_list(dict_with_topics: dict[Any, Any] | Any) -> list[str]:
    """
    This function extracts instagram your_topics from a dict
    This dict should be obtained from your_topics.json

    This function should be rewritten as your_topics.json changes
    """
    out = []

    try:
        if not isinstance(dict_with_topics, dict):
            raise TypeError("The input to this function was not dict")

        # The compleet lookup is:
        # "topics_your_topics" -> "string_map_data" -> "Name" -> "value"
        # Dutch Language DDP is: "topics_your_topics" -> "string_map_data" -> "Naam" -> "value"
        # Note: popitem avoids hardcoding "Name" or "Naam"
        for item in dict_with_topics["topics_your_topics"]:
            res = item["string_map_data"].popitem()
            out.append(res[1]["value"])

    except TypeError as e:
        logger.error("TypeError: %s", e)
    except KeyError as e:
        logger.error("The a dict did not contain the key: %s", e)
    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    finally:
        return out


def account_created_at_to_list(account_info_dict: dict[Any, Any] | Any) -> list[str]:
    """
    Returns account created at timestamp in iso format

    This function flattens the json structure first.
    Flattening the json structure first allows you to look up
    keys starting from the back. Only the Last N keys matter for the look up.

    Note:
        * Decide later whether this code can be made more generally available
        * Add search keys in the list below
    """
    search_keys_list = [
            ["Tijd", "timestamp"],
            ["Time", "timestamp"],
        ]
    out = []

    try:
        d = scanfiles.dict_denester(account_info_dict)

        for (k, v), search_keys in product(d.items(), search_keys_list):
            pattern = re.compile(f"^.*{'_'.join(search_keys)}$")
            if pattern.match(k):
                out = [stringparse.epoch_to_iso(v)]
                break
        else:
            logger.info("Search keys not found: No timestamp to convert")

    except CannotConvertEpochTimestamp as e:
        logger.error("Cannot convert timestamp: %s", e)
    except Exception as e:
        logger.error("Exception: %s", e)

    finally:
        return out
