"""
This module contains functions to classify strings found in data download packages
"""

from datetime import datetime, timezone
import ipaddress
import warnings
import logging
import re

import pandas as pd

from port.parserlib import urldetectionregex

logger = logging.getLogger(__name__)


class CannotConvertEpochTimestamp(Exception):
    """"Raise when epoch timestamp cannot be converted to isoformat"""


def is_timestamp(input_string: str) -> bool:
    """
    Detects if string is a timestamp
    relies on pandas.to_datetime() to detect the time format
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("error")  # temporary behaviour

        try:
            assert isinstance(input_string, str)
            assert input_string != ""
            assert input_string.isdigit() is False

            pd.to_datetime(input_string)

            logger.debug("timestamp FOUND in: '%s'", input_string)
            return True

        except (ValueError, AssertionError) as e:
            logger.debug("Timestamp NOT found in: '%s', %s", input_string, e)
            return False

        except Warning as e:
            logger.warning(
                "WARNING was raised as exception "
                "probably NO timestamp in: "
                "'%s', %s",
                input_string,
                e,
            )
            return False

        except Exception as e:
            logger.error(e)
            return False


def has_url(input_string: str, exact: bool = False) -> bool:
    """
    Detects if string contains urls, use exact is True if the string is a url
    see ./urldetectionregex for the regexes

    Note: I tried the package: urlextractor which is too slow
    regex is magnitudes faster
    """
    try:
        regex = (
            urldetectionregex.URL_REGEX
            if exact is False
            else urldetectionregex.URL_REGEX_MATCH_BEGIN_AND_ENDLINE
        )
        assert re.search(regex, input_string) is not None
        logger.debug("urls FOUND in: '%s'", input_string)
        return True

    except AssertionError as e:
        logger.debug("%s, urls NOT found in: '%s'", e, input_string)
        return False

    except Exception as e:
        logger.error(e)
        return False


def has_email(input_string: str, exact: bool = False) -> bool:
    """
    Detects if string contains emails
    use exact is True if the string is an email
    see ./urldetectionregex for the regexes
    """
    try:
        regex = (
            urldetectionregex.EMAIL_REGEX
            if exact is False
            else urldetectionregex.EMAIL_REGEX_MATCH_BEGIN_AND_ENDLINE
        )
        assert re.search(regex, input_string) is not None
        logger.debug("emails FOUND in: '%s'", input_string)
        return True

    except AssertionError as e:
        logger.debug("%s, emails NOT found in: '%s'", e, input_string)
        return False

    except Exception as e:
        logger.error(e)
        return False


def is_ipaddress(input_string: str) -> bool:
    """
    Detects if string is a valid IPv4 or IPv6 address returns bool
    """
    try:
        assert isinstance(input_string, str)
        ipaddress.ip_address(input_string)
        logger.debug("IP found in string: '%s'", input_string)
        return True

    except (ValueError, AssertionError) as e:
        logger.debug("%s, IP NOT found in string: '%s'", e, input_string)
        return False

    except Exception as e:
        logger.error(e)
        return False


def is_isoformat(
    datetime_str: list[str] | list[int], check_minimum: int, date_only: bool = False
) -> bool:
    """
    Check if list like object containing datetime stamps are ISO 8601 strings
    date_only = True, checks if only the date part is ISO 8601
    """

    regex = (
        urldetectionregex.REGEX_ISO8601_FULL
        if date_only is False
        else urldetectionregex.REGEX_ISO8601_DATE
    )

    try:
        for i in range(min(len(datetime_str), check_minimum)):
            if isinstance(datetime_str[i], int):
                logger.debug(
                    "Could not detect ISO 8601 timestamp (date_only=%s): %s",
                    date_only,
                    datetime_str[i],
                )
                return False

            if re.fullmatch(regex, datetime_str[i]) is None:  # type: ignore
                logger.debug(
                    "Could not detect ISO 8601 timestamp (date_only=%s): %s",
                    date_only,
                    datetime_str[i],
                )
                return False

    except Exception as e:
        logger.debug(
            "Could not detect ISO 8601 timestamp (date_only=%s): %s, error: %s",
            date_only,
            datetime_str[i],
            e
        )
        return False

    logger.debug("ISO 8601 timestamp detected (date_only=%s)", date_only)
    return True


def is_epoch(datetime_int: list[int] | list[str], check_minimum: int) -> bool:
    """
    Check if list-like object with ints or str that can be interpreted as ints
    epoch time (unit seconds) fall between the start of year 2000 and the year 2040
    """

    year2000 = 946684800
    year2040 = 2208988800

    try:
        for i in range(min(len(datetime_int), check_minimum)):
            check_time = int(datetime_int[i])
            if not year2000 <= check_time <= year2040:
                logger.debug("Could not detect epoch time timestamp: %s", check_time)
                return False

    except Exception as e:
        logger.debug("Could not detect epoch time timestamp, %s", e)
        return False

    logger.debug("Epoch timestamp detected")
    return True


def epoch_to_iso(epoch_timestamp: str | int) -> str:
    """
    Convert epoch timestamp to an ISO 8601 string. Assumes UTC.

    If timestamp cannot be converted raise CannotConvertEpochTimestamp
    """
    try:
        epoch_timestamp = int(epoch_timestamp)
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)
        raise CannotConvertEpochTimestamp("Cannot convert epoch timestamp") from e

    return out


def convert_datetime_str(datetime_str: list[str] | list[int]) -> pd.DatetimeIndex | None:
    """
    If timestamps are ISO 8601 return those
    If timestamps are ints (epochtime) return those

    If first ambigous non NaN timestamps is encoutered
    (and can be detected with infer_datetime_format=True and dayfirst-True)
    every timestamp is interpreted as DD/MM/YYYY if possible,
    if resulting in incorrect timestamp, then MM/DD/YYYY is used,
    converting format is silently switched

    If first non-ambigous non NaN timestamps is encoutered
    (and can be detected with infer_datetime_format=True and dayfirst-True)
    every timestamp is interpreted as either DD/MM/YYYY or MM/DD/YYYY depending on the first timestamp
    If the other format is encountered, that would result in an error, format is silently switched

    If timestamp format cannot be detected with infer_datetime_format
    dateutils.parser.parse() is used with dayfirst is true setting
    Interpretering everything as DD/MM/YYYY except if that results in an impossible date,
    then MM/DD/YYYY is used

    YYYY/MM/DD formats not ISO 8601 will be interpreted incorrectly, as YYYY/DD/MM if possible

    Note 1: to_datetime will be rewritten in a future pandas release
    Also to_datetime, guess_datetime_format will be made available in pandas.tools
    The silent format changes will be changed.

    Note 2: This is a very complicated problem to solve, solving this problem myself is too difficult
    for what I might expect to gain in accuracy

    Note 3: if time is converted from epoch utc is used

    Concluding: Although a lot can go wrong, I expect the impact will be minor,
    When american formats are encourted regularly things will go wrong most often
    """
    out = None
    try:
        if is_isoformat(datetime_str, 10):
            out = pd.to_datetime(datetime_str)

        elif is_isoformat(datetime_str, 10, date_only=True):
            out = pd.to_datetime(datetime_str)

        elif is_epoch(datetime_str, 10):
            out = pd.to_datetime(datetime_str, unit="s")

        else:
            out = pd.to_datetime(
                datetime_str, infer_datetime_format=True, dayfirst=True
            )

    except (ValueError, TypeError, OverflowError) as e:
        logger.error("Could not convert timestamps: %s", e)

    finally:
        return out
