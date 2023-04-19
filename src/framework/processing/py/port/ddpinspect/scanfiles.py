"""
This module contains functions to scan the files contained inside a DDP
"""

from pathlib import Path
from pathlib import PurePath
from typing import Any
from datetime import datetime
from dataclasses import dataclass, asdict
import logging
import json
import uuid

import magic
import pandas as pd

from port.parserlib import stringparse

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JsonItemDescription:
    """
    Class containing the description of Json items
    """

    name: str
    object_id: str
    parent_id: str
    object_type: str
    object_value: str
    is_ip: bool | None
    is_time: bool | None
    is_url: bool | None


@dataclass(frozen=True)
class FileDescription:
    """
    Class containing the description of files
    """

    name: str
    parent: str
    suffix: str
    is_directory: bool
    modification_time: str
    file_size: int
    file_description: str | None
    mime_type: str | None


def path_exists(p: Path) -> None:
    """Checks if path exists"""
    if p.exists():
        return None
    raise FileNotFoundError(f"Path: {p} does not exists")


def read_json_from_file(path_to_json: Path) -> dict[Any, Any] | list[Any]:
    """
    Reads json from file if succesful it returns the result from json.load()
    """
    path_to_json = Path(path_to_json)
    out: dict[Any, Any] | list[Any]

    try:
        with open(path_to_json, encoding="utf8") as f:
            out = json.load(f)
        logger.debug("succesfully opened: %s", path_to_json.name)

    except json.JSONDecodeError:
        try:
            with open(path_to_json, encoding="utf-8-sig") as f:
                out = json.load(f)
            logger.debug("succesfully opened: %s", path_to_json.name)
        except Exception as e:
            logger.error("%s, could not open: %s", e, path_to_json)
            raise e

    except Exception as e:
        logger.error("%s, could not open: %s", e, path_to_json)
        raise e

    return out


def flatten_nested_dict(
    obj: Any, name: str, parent_id: str, out: list[JsonItemDescription]
) -> list[JsonItemDescription]:
    """
    Recursive function that flattens a nested dict (resulting from json.load(s))
    Returns the flat items in the JSON and classifies them
    """

    object_id = uuid.uuid4().hex
    parent_id = parent_id + "/" + object_id
    is_ip = is_time = is_url = None

    if isinstance(obj, dict):
        obj = dict(sorted(obj.items()))
        object_value = ":".join([str(k) for k in obj.keys()])
        for k, v in obj.items():
            flatten_nested_dict(obj=v, name=str(k), parent_id=parent_id, out=out)

    elif isinstance(obj, list):
        object_value = "List"
        for index, item in enumerate(obj):
            flatten_nested_dict(
                obj=item, name=f"{name}_{index}", parent_id=parent_id, out=out
            )

    elif isinstance(obj, str):
        object_value = obj
        is_ip = stringparse.is_ipaddress(obj)
        is_time = stringparse.is_timestamp(obj)
        is_url = stringparse.has_url(obj)

    else:
        object_value = str(obj)

    out.append(
        JsonItemDescription(
            name,
            object_id,
            parent_id,
            type(obj).__name__,
            object_value,
            is_ip,
            is_time,
            is_url,
        )
    )

    return list(reversed(out))


def flatten_json(path_to_json: Path) -> dict[str, Any]:
    """
    Returns flattened json
    """

    path_to_json = Path(path_to_json)
    path_exists(path_to_json)

    last_modified = datetime.fromtimestamp(path_to_json.stat().st_mtime).isoformat()

    json_obj = read_json_from_file(path_to_json)
    json_items = flatten_nested_dict(json_obj, "toplevel", "", [])

    out = {
        "name": path_to_json.name,
        "last_modified": last_modified,
        "json_items": json_items,
    }

    return out


def flatten_json_all(foldername: Path) -> pd.DataFrame:

    """
    Reads contents of all json files in a folder recursively
    Returns a pandas dataframe
    """

    foldername = Path(foldername)
    path_exists(foldername)

    try:
        out = []
        paths = foldername.glob("**/*.json")

        # For all jsons in folder: flatten them and assemble in pandas df
        for p in paths:
            json_flat = flatten_json(p)
            json_items = [asdict(item) for item in json_flat["json_items"]]
            for d in json_items:
                d.update(
                    {
                        "filename": json_flat["name"],
                        "last_modified": json_flat["last_modified"],
                    }
                )

            out.extend(json_items)

        df = pd.DataFrame(out)

        return df

    except Exception as e:
        logger.critical(e)
        raise e


def scan_files_all(foldername: Path) -> pd.DataFrame:
    """
    Recursively examines all files in folder
    and collects meta data about that file
    """

    file_descriptions = []
    foldername = Path(foldername)
    path_exists(foldername)

    paths = foldername.glob("**/*")
    for p in paths:
        try:
            name = p.name
            parent = str(PurePath(p.parent).relative_to(foldername.parent))
            suffix = " ".join(p.suffixes)
            is_directory = p.is_dir()

            # Obtain file statistics
            filestats = p.stat()
            modification_time = datetime.fromtimestamp(filestats.st_mtime).isoformat()
            file_size = filestats.st_size

            # Magic is equivalent to the unix "file" command
            file_description = mime_type = None
            if not is_directory:
                file_description = magic.from_file(p)
                mime_type = magic.from_file(p, mime=True)

            file_descriptions.append(
                FileDescription(
                    name,
                    parent,
                    suffix,
                    is_directory,
                    modification_time,
                    file_size,
                    file_description,
                    mime_type,
                )
            )

            logger.debug("Examined file/folder: %s", p)
        except Exception as e:
            logger.error("%s, could not examine file/folder %s", e, p)
            raise e

    df = pd.DataFrame([asdict(fd) for fd in file_descriptions])
    return df


def dict_denester(
    inp: dict[Any, Any] | list[Any],
    new: dict[Any, Any] | None = None,
    name: str = "",
    run_first: bool = True,
) -> dict[Any, Any]:
    """
    Denest a dict or list, returns a new denested dict
    """

    if run_first:
        new = {}

    if isinstance(inp, dict):
        for k, v in inp.items():
            if isinstance(v, (dict, list)):
                dict_denester(v, new, f"{name}_{str(k)}", run_first=False)
            else:
                newname = f"{name}_{k}"
                new.update({newname[1:]: v})  # type: ignore

    elif isinstance(inp, list):
        for i, item in enumerate(inp):
            dict_denester(item, new, f"{name}_{i}", run_first=False)

    else:
        new.update({name[1:]: inp})  # type: ignore

    return new  # type: ignore


def remove_const_cols_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all columns that have constant values
    if the number of rows is larger than 1
    """
    if len(df.index) > 1:
        cols = df.columns[df.nunique(dropna=False) >= 2]
        df = df[cols]

    return df
