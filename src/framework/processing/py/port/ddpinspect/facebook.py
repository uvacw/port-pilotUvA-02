"""
DDP facebook module

This module contains functions to handle *.jons files contained within a facebook ddp
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
            "events_interactions.json",
            "group_interactions.json",
            "people_and_friends.json",
            "advertisers_using_your_activity_or_information.json",
            "advertisers_you've_interacted_with.json",
            "apps_and_websites.json",
            "your_off-facebook_activity.json",
            "comments.json",
            "posts_and_comments.json",
            "event_invitations.json",
            "your_event_responses.json",
            "accounts_center.json",
            "marketplace_notifications.json",
            "payment_history.json",
            "controls.json",
            "reduce.json",
            "friend_requests_received.json",
            "friend_requests_sent.json",
            "friends.json",
            "rejected_friend_requests.json",
            "removed_friends.json",
            "who_you_follow.json",
            "your_comments_in_groups.json",
            "your_group_membership_activity.json",
            "your_posts_in_groups.json",
            "primary_location.json",
            "primary_public_location.json",
            "timezone.json",
            "notifications.json",
            "pokes.json",
            "ads_interests.json",
            "friend_peer_group.json",
            "pages_and_profiles_you_follow.json",
            "pages_and_profiles_you've_recommended.json",
            "pages_and_profiles_you've_unfollowed.json",
            "pages_you've_liked.json",
            "polls_you_voted_on.json",
            "your_uncategorized_photos.json",
            "your_videos.json",
            "language_and_locale.json",
            "live_video_subscriptions.json",
            "profile_information.json",
            "profile_update_history.json",
            "your_local_lists.json",
            "your_saved_items.json",
            "your_search_history.json",
            "account_activity.json",
            "authorized_logins.json",
            "browser_cookies.json",
            "email_address_verifications.json",
            "ip_address_activity.json",
            "login_protection_data.json",
            "logins_and_logouts.json",
            "mobile_devices.json",
            "record_details.json",
            "where_you're_logged_in.json",
            "your_facebook_activity_history.json",
            "archived_stories.json",
            "location.json",
            "recently_viewed.json",
            "recently_visited.json",
            "your_topics.json",
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


def interests_to_list(dict_with_interests: dict[Any, Any] | Any) -> list[str]:
    """
    This function extracts instagram interests from a dict
    This dict should be obtained from ads_interests.json
    """
    out = []

    try:
        if not isinstance(dict_with_interests, dict):
            raise TypeError("The input to this function was not dict")

        out = dict_with_interests["topics_v2"]

    except TypeError as e:
        logger.error("The input list did not contain a dict: %s", e)
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

        out = dict_with_topics["inferred_topics_v2"]

    except TypeError as e:
        logger.error("The input list did not contain a dict: %s", e)
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
            ["profile_v2", "registration_timestamp"],
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
