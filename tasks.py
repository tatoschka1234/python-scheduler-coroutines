from pathlib import Path
import requests
from utils import get_logger

logger = get_logger()


def read_file() -> None:
    data = yield
    logger.info(f"Going to read file {data['file_name']}")
    try:
        f = open(data['file_name'])
    except FileNotFoundError as ex:
        logger.debug(str(ex))
        raise ValueError(str(ex))

    try:
        for row in f:
            yield row
            logger.debug(f"reading row={row}")
    finally:
        logger.debug(f"File {data['file_name']} was closed")
        f.close()


def create_file() -> None:
    data = yield
    logger.info(f"Going to create file {data['file_name']}")
    try:
        with open(data['file_name'], 'x'):
            pass
    except FileExistsError as ex:
        raise ValueError(str(ex))


def write_to_file() -> None:
    data = yield
    logger.info(f"Going to write to file {data['file_name']}")
    with open(data['file_name'], 'w') as f:
        for line in data["lines"]:
            f.write(line + "\n")
            yield


def create_folder() -> None:
    """
    Creates folder in working dir
    """
    data = yield
    logger.info(f"Going to create folder {data['dir_name']}")
    try:
        Path(data['dir_name']).mkdir()
    except FileExistsError as ex:
        raise ValueError(str(ex))


def delete_folder() -> None:
    """
    Removes folder in working folder
    """
    data = yield
    logger.info(f"Going to delete folder {data['dir_name']}")
    try:
        Path(data['dir_name']).rmdir()
    except FileNotFoundError as ex:
        raise ValueError(str(ex))
    except OSError as ex:
        raise ValueError(str(ex))


def rename_object() -> Path:
    """Rename given file or directory to the given target,
    and return a new Path instance pointing to target"""
    data = yield
    logger.info(f"Going to rename object {data['obj_name']}")
    try:
        p = Path(data['obj_name'])
        return p.rename(data['target_name'])
    except FileExistsError as ex:
        raise ValueError(str(ex))
    except FileNotFoundError as ex:
        raise ValueError(str(ex))


def get_url() -> None:
    """
    Send get request
    """
    data = yield
    logger.info(f"Going to send http get to {data['url']}:")
    try:
        result = requests.get(data["url"])
        if result.status_code != data["expected_code"]:
            raise ValueError(f"Unexpected status code: {result.status_code}")
    except requests.ConnectionError as ex:
        raise ValueError(str(ex))
