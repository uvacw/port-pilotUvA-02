"""
DDP Twitter module

This module contains functions to handle *.js files contained within a twitter ddp
"""

from pathlib import Path
from typing import Any
import logging
import zipfile
import json
import io
import re

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
        id="js_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "manifest.js",
            "account-creation-ip.js",
            "account-label.js",
            "account-suspension.js",
            "account-timezone.js",
            "account.js",
            "ad-engagements.js",
            "ad-free-article-visits.js",
            "ad-impressions.js",
            "ad-mobile-conversions-attributed.js",
            "ad-mobile-conversions-unattributed.js",
            "ad-online-conversions-attributed.js",
            "ad-online-conversions-unattributed.js",
            "ageinfo.js",
            "app.js",
            "birdwatch-note-rating.js",
            "birdwatch-note-tombstone.js",
            "birdwatch-note.js",
            "block.js",
            "branch-links.js",
            "catalog-item.js",
            "commerce-catalog.js",
            "community-tweet.js",
            "connected-application.js",
            "contact.js",
            "deleted-tweet.js",
            "device-token.js",
            "direct-message-group-headers.js",
            "direct-message-headers.js",
            "direct-message-mute.js",
            "direct-messages-group.js",
            "direct-messages.js",
            "email-address-change.js",
            "follower.js",
            "following.js",
            "ip-audit.js",
            "like.js",
            "lists-created.js",
            "lists-member.js",
            "lists-subscribed.js",
            "moment.js",
            "mute.js",
            "ni-devices.js",
            "periscope-account-information.js",
            "periscope-ban-information.js",
            "periscope-broadcast-metadata.js",
            "periscope-comments-made-by-user.js",
            "periscope-expired-broadcasts.js",
            "periscope-followers.js",
            "periscope-profile-description.js",
            "personalization.js",
            "phone-number.js",
            "product-drop.js",
            "product-set.js",
            "professional-data.js",
            "profile.js",
            "protected-history.js",
            "reply-prompt.js",
            "saved-search.js",
            "screen-name-change.js",
            "shop-module.js",
            "shopify-account.js",
            "smartblock.js",
            "spaces-metadata.js",
            "sso.js",
            "tweet.js",
            "tweetdeck.js",
            "twitter-circle-member.js",
            "twitter-circle-tweet.js",
            "twitter-circle.js",
            "twitter-shop.js",
            "user-link-clicks.js",
            "verified.js",
        ],
    )
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
                if p.suffix in (".js"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validate.set_status_code(0)
        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_status_code(1)

    return validate


def bytesio_to_listdict(bytes_to_read: io.BytesIO) -> list[dict[Any, Any]]:
    """
    Converts a io.BytesIO buffer containing a twitter.js file, to a list of dicts

    A list of dicts is the current structure of twitter.js files
    """

    out = []
    lines = []

    try:
        with io.TextIOWrapper(bytes_to_read, encoding="utf8") as f:
            lines = f.readlines()

        # change first line so its a valid json
        lines[0] = re.sub("^.*? = ", "", lines[0])

        # convert to a list of dicts
        out = json.loads("".join(lines))

    except json.decoder.JSONDecodeError as e:
        logger.error("The input buffer did not contain a valid JSON: %s", e)
    except IndexError as e:
        logger.error("No lines were read, could be empty input buffer: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out


def interests_to_list(interest_list: list[dict[Any, Any]]) -> list[str]:
    """
    This function extracts twitter interests from a list[dict]
    This list[dict] should be obtained from personalization.js

    This function should be rewritten as personalization.js changed
    """
    out = []

    try:
        dict_with_interests = interest_list[0]

        if not isinstance(dict_with_interests, dict):
            raise TypeError("The first item in interest_list is not a dict")

        # traverse into the nested dict
        for key in ["p13nData", "interests", "interests"]:
            dict_with_interests = dict_with_interests[key]

        out = [d.get("name") for d in dict_with_interests]

    except IndexError as e:
        logger.error("The input object is an empty list: %s", e)
    except TypeError as e:
        logger.error("The input list did not contain a dict: %s", e)
    except KeyError as e:
        logger.error("The a dict did not contain the correct lookup key: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out


def account_created_at_to_list(account_info_list: list[dict[Any, Any]]) -> list[str]:
    """
    Returns the account creation date from account.js
    """
    out = []

    try:
        dict_with_account_info = account_info_list[0]

        if not isinstance(dict_with_account_info, dict):
            raise TypeError("The first item in interest_list is not a dict")

        dict_with_account_info = dict_with_account_info["account"]
        out = [dict_with_account_info["createdAt"]]

    except IndexError as e:
        logger.error("The input object is an empty list: %s", e)
    except TypeError as e:
        logger.error("The input list did not contain a dict: %s", e)
    except KeyError as e:
        logger.error("The a dict did not contain the correct lookup key: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out
