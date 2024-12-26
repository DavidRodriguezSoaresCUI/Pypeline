import json
import logging
import random
import re
import string
from datetime import datetime
from enum import Enum
from pathlib import Path
from time import time
from traceback import format_exception
from typing import Any, Callable, Generic, TypeVar

from DRSlib.logging import LOG_FORMAT_BASIC

PYPELINE_LOGGER = "PYPELINE"
ID_CHARACTERS = string.digits + string.ascii_uppercase
LOG_FORMAT_WITH_TIME = "[%(asctime)s]" + LOG_FORMAT_BASIC
LOG_SEPARATOR = "#" * 10


def debug(something: Any, ending: str = "\n") -> None:
    with Path("dump.json").open("a", encoding="utf8") as f:
        f.write(
            json.dumps(
                something,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
            + ending
        )


def full_exception(ex: Exception) -> str:
    """Formats exception like Python would, and make it save as JSON value ()"""
    return "".join(format_exception(type(ex), ex, ex.__traceback__, limit=3))


def make_JSON_string_safe(s: str) -> str:
    """Ensures no illegal character for JSON string is present"""
    return s.replace("\n", "\\n").replace('"', "'")


def add_file_handler(
    log: logging.Logger, log_file: Path, log_level: int = logging.INFO
) -> None:
    """Adds to a logger the ability to log to a file"""
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT_WITH_TIME))
    file_handler.setLevel(log_level)
    log.addHandler(file_handler)


def remove_file_handlers(log: logging.Logger) -> None:
    """Remove file handlers for this logger"""
    for handler in list(log.handlers):
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            handler.close()
            log.removeHandler(handler)


def change_formatters(log: logging.Logger, fmt: str) -> None:
    """Change all handlers' formatter"""
    formatter = logging.Formatter(fmt)
    for handler in log.handlers:
        handler.setFormatter(formatter)


def datetime_to_cron_day(_time: datetime) -> int:
    """Returns the cron-style day for a datetime
    datetime weekday representation is 0:monday .. 6:sunday and cron's is 0:sunday .. 6:saturday
    """
    return (_time.weekday() + 1) % 7


# def expect_exactly_one(iterable: Iterable) -> Any:
#     """Returns only element, unless iterable doesn't contain exactly one element; then raise Exception"""
#     values = list(iterable)
#     if len(values) == 1:
#         return values[0]
#     raise ValueError(f"Expected exactly one element, got {len(values)}")


class TimeResolution(Enum):
    MINUTE = 60
    HOUR = 24
    DAY = 7


def clone_datetime(_time: datetime, resolution: TimeResolution) -> datetime:
    match resolution:
        case TimeResolution.MINUTE:
            return datetime(
                year=_time.year,
                month=_time.month,
                day=_time.day,
                hour=_time.hour,
                minute=_time.minute,
            )
        case TimeResolution.HOUR:
            return datetime(
                year=_time.year, month=_time.month, day=_time.day, hour=_time.hour
            )
        case TimeResolution.DAY:
            return datetime(year=_time.year, month=_time.month, day=_time.day)
        case _:
            raise ValueError(f"Unsupported value for resolution: {resolution}")


def strip_values(values: list[str]) -> list[str]:
    """Strips every item in a list"""
    return [x.strip() for x in values]


def random_base32(n: int) -> str:
    """Returns a base32-like string with n symbols"""
    if n <= 0:
        raise ValueError(f"Argument n must be strictly positive, given {n}")
    m = len(ID_CHARACTERS) - 1
    return "".join(ID_CHARACTERS[random.randint(0, m)] for _ in range(n))  # nosec B311


class Singleton(type):
    """Implements singleton pattern via metaclass"""

    _instance: Any = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance

    def get_instance(cls) -> Any:
        return cls._instance


T = TypeVar("T")


class FileDefinedValue(Generic[T]):
    """For data defined in a file that may be updated, this class provides a seamless interface to retrieve it"""

    source_file: Path
    """This file is the source of data and must be queried on changes"""
    last_read: int
    """In order to avoid fetching data from disk every time, we keep track of last read"""
    data: T
    """The data that was read most recently"""
    data_parsing_function: Callable[[Path], T]

    def __init__(
        self, source_file: Path, data_parsing_function: Callable[[Path], T]
    ) -> None:
        self.source_file = source_file
        self.data_parsing_function = data_parsing_function
        self.last_read = 0

    def reload_data(self) -> None:
        """Conditionally reloads data"""
        if self.should_reload_data():
            self.data = self.data_parsing_function(self.source_file)
            self.last_read = int(time())

    def should_reload_data(self) -> bool:
        """Data should be reloaded if never loaded or if source file was modified"""
        return self.last_read == 0 or self.last_read < self.source_file.stat().st_mtime

    def get(self) -> T:
        """Main interface, fetches the data"""
        self.reload_data()
        return self.data
