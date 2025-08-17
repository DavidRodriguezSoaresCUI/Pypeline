from io import StringIO
import json
import logging
import random
import string
from datetime import datetime
from enum import Enum
from pathlib import Path
from time import time
from traceback import format_exception
from typing import Any, Callable, Generic, TypeVar

from drslib.logging import LOG_FORMAT_BASIC
from ruamel.yaml import YAML

PYPELINE_LOGGER = "PYPELINE"
ID_CHARACTERS = string.digits + string.ascii_uppercase
LOG_FORMAT_WITH_TIME = "[%(asctime)s]" + LOG_FORMAT_BASIC
LOG_SEPARATOR = "#" * 10


class OrchestratorReturnCode(Enum):
    """Orchestrator may exit main loop for either reasons"""

    EXIT_OK = 0
    RESERVED_ERROR = 1
    RELOAD = 2


def debug(something: Any, ending: str = "\n") -> None:
    """For when debugging data can't be done via a print/log (too large)"""
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
    """Formats exception like Python would"""
    return "".join(format_exception(type(ex), ex, ex.__traceback__, limit=3))


def make_string_json_safe(s: str) -> str:
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


class TimeResolution(Enum):
    """Time resolutions used in CRONlite"""

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


def make_banner(title: str) -> str:
    """Returns a "lean style" banner in string representation"""
    banner_width = len(title) + 4
    return "\n".join(["#" * banner_width, f"# {title} #", "#" * banner_width])


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
        if not source_file.exists():
            source_file.touch()
        self.source_file = source_file
        self.data_parsing_function = data_parsing_function
        self.last_read = 0

    def __reload_data(self) -> None:
        """Conditionally reloads data"""
        if self.should_reload_data():
            self.data = self.data_parsing_function(self.source_file)
            self.last_read = int(time())

    def should_reload_data(self) -> bool:
        """Data should be reloaded if never loaded or if source file was modified"""
        return self.last_read == 0 or self.last_read < self.source_file.stat().st_mtime

    def get(self) -> T:
        """Main interface, fetches the data"""
        self.__reload_data()
        return self.data

    def edit_content(self, editor: Callable[[str], str]) -> None:
        """Edit source file content (loads content as utf8)"""
        self.source_file.write_text(
            editor(self.source_file.read_text(encoding="utf8")), encoding="utf8"
        )


def str_representer(dumper, data):
    """Used to enable multiline string support in YAML dump
    source : https://gist.github.com/alertedsnake/c521bc485b3805aa3839aef29e39f376
    """
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def dump_to_yaml(obj: object) -> str:
    """Dump object to YAML string representation"""

    stream = StringIO()
    yaml = YAML()
    # yaml.default_flow_style = False
    yaml.encoding = "utf-8"
    yaml.representer.add_representer(str, str_representer)
    yaml.dump(obj, stream)

    return stream.getvalue()
