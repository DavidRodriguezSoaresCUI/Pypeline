from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
import logging
from pathlib import Path
import re

from DRSlib.path_tools import ensure_dir_exists

from .utils import PYPELINE_LOGGER

LOG = logging.getLogger(PYPELINE_LOGGER)

VALID_ACTIVITY_TYPE_PATTERN = re.compile(r"^[a-zA-Z_\-]{5,40}$")
VALID_ACTIVITY_TYPE_PATTERN_HELP = (
    "Must use A-Z symbols plus '-_' and use 5-40 characters"
)


class ExitStatus(Enum):
    """Represents all possible Processor exit status"""

    SUCCESS = auto()
    DECLINED = auto()
    ERROR_RETRY = auto()
    ERROR_ABANDON = auto()


class ActivityState(Enum):
    """Represents the different states an activity can be in"""

    TO_BE_PROCESSED = auto()
    """Activity is ready to be processed"""
    IN_PROGRESS = auto()
    """Activity is being processed"""
    PROCESSED = auto()
    """Activity has successfully been processed"""
    ERROR = auto()
    """Activity failed to be processed"""
    # Ignored = auto()
    # """Activity was marked to not be processed"""


@dataclass
class ExitState:
    """Date returned by a process with information about the execution and the activity's
    state transition"""

    status: ExitStatus
    """exit status"""
    reason: str | None
    """Optional message explaining the status, typically exclusive to non-success states"""
    next_activity_status: ActivityState
    """Used to for activity status change"""
    actual_work_was_done: bool
    """Used to know if the activity files may be discarded """

    @staticmethod
    def success(
        reason: str | None = None, actual_work_was_done: bool = True
    ) -> "ExitState":
        """Create a new ExitState for typical successful process executions"""
        return ExitState(
            ExitStatus.SUCCESS, reason, ActivityState.PROCESSED, actual_work_was_done
        )

    @staticmethod
    def declined(
        reason: str,
        next_activity_status: ActivityState = ActivityState.TO_BE_PROCESSED,
        actual_work_was_done: bool = False,
    ) -> "ExitState":
        """Create a new ExitState for typical process executions that declined to process
        the activity"""
        return ExitState(
            ExitStatus.DECLINED, reason, next_activity_status, actual_work_was_done
        )

    @staticmethod
    def retry(reason: str, actual_work_was_done: bool = False) -> "ExitState":
        """Create a new ExitState for typical process executions that encountered an error"""
        return ExitState(
            ExitStatus.ERROR_RETRY,
            reason,
            ActivityState.TO_BE_PROCESSED,
            actual_work_was_done,
        )

    @staticmethod
    def error(reason: str, actual_work_was_done: bool = True) -> "ExitState":
        """Create a new ExitState for typical process executions that encountered an error"""
        return ExitState(
            ExitStatus.ERROR_ABANDON, reason, ActivityState.ERROR, actual_work_was_done
        )

    @property
    def remove_activity(self) -> bool:
        """Returns True on states that warrant the activity to be removed"""
        return not self.actual_work_was_done and self.status is ExitStatus.SUCCESS

    def __str__(self) -> str:
        """Returns a string representation"""
        return (
            "{"
            + f"status:{self.status.name},"
            + (f"reason:{self.reason}," if self.reason else "")
            + f"next_activity_status:{self.next_activity_status},"
            + f"actual_work_was_done:{self.actual_work_was_done}"
            + "}"
        )


class Activity:
    """
    Each activity is stored as a file in a reserved location:

    - path: ``<rootDir>/<state>/activity.<type>.<id>[.<retry>].json``
    - attributes:
    - id (integer base 10 or higher TBD)
    - type (to match processor)
    - state
    - input objects (content of the file)
    - Only exist as an abstraction for the input data to a processing step
    - Can conceptually be sorted into 3 groups:
    - bootstrap activities: no input, only exist so associated processor can be executed
      and potentially create other activities
    - step activities: do actual work with input objects, optionally create further
      activities (including bootstrap and terminal)
    - terminal activity: perform additional actions (logging, cleanup, etc) but never
      create further activities

    While an activity file's contents are assumed to be WORM data, the file representation
    allows for easily changing activities' state by moving the associated file, creation
    by creating a new file, removal by deleting the file, etc. Only a user should edit
    an activity file's contents, and then only in exceptional cases.

    Note: it is assumed that

    TBD: are there any reasons to add additional info, like execution context, creation
    date, history, creation processor, owner (user), whether it is part of a recurrent
    or "on demand" process, etc ?

    activity.ReadDSNotifs.2024-01-01T16-30.JFU_0
    """

    activity_type: str
    """Used to match the activity with an appropriate processor"""
    creation_time: datetime
    """Creation date time (precision: minute, represented as YYYY-MM-DDTHH-mm)"""
    activity_id: str
    """Unique identifier and "short name" for the activity"""
    retries: int
    """Number of retries for this activity (starts at 0)"""
    state: ActivityState
    """State of the activity, represented by containing directory name"""
    data: str
    """JSON-encoded data"""

    FILE_NAME_PATTERN = re.compile(
        r"activity\.([^\.]+?)\.([0-9T-]+?)\.([^\.]+)_(\d+)\.json"
    )  # <type:str>, <create-datetime:str>, <id:str>, <retry:int>
    CREATION_TIME_FORMAT = "%Y-%m-%dT%H-%M"

    def __init__(
        self,
        activity_type: str,
        creation_time: datetime | str,
        activity_id: str,
        retries: int,
        state: str | ActivityState,
        data: str,
    ) -> None:
        self.activity_type = activity_type
        self.creation_time = (
            creation_time
            if isinstance(creation_time, datetime)
            else datetime.strptime(creation_time, self.CREATION_TIME_FORMAT)
        )
        self.activity_id = activity_id
        self.retries = retries
        self.state = ActivityState[state] if isinstance(state, str) else state
        self.data = data

    @classmethod
    def match_activity_file_name(cls, activity_file_name: str) -> re.Match:
        """Returns a match on filename for id or activity type extraction"""
        _match = cls.FILE_NAME_PATTERN.match(activity_file_name)
        if not _match:
            raise ValueError(
                f"Invalid name '{activity_file_name}' doesn't match pattern {cls.FILE_NAME_PATTERN.pattern}"
            )
        return _match

    @classmethod
    def get_retry_count(cls, activity_file_name: str) -> int:
        """Get activity id from file name"""
        retries: str = cls.match_activity_file_name(activity_file_name).group(4)
        return int(retries) if retries is not None and retries.isdigit() else 0

    @classmethod
    def get_id(cls, activity_file_name: str) -> str:
        """Get activity id from file name"""
        return cls.match_activity_file_name(activity_file_name).group(3)

    @classmethod
    def get_creation_date(cls, activity_file_name: str) -> datetime:
        """Get activity id from file name"""
        return datetime.strptime(
            cls.match_activity_file_name(activity_file_name).group(2),
            cls.CREATION_TIME_FORMAT,
        )

    @classmethod
    def get_type(cls, activity_file_name: str) -> str:
        """Get activity type from file name"""
        return cls.match_activity_file_name(activity_file_name).group(1)

    @classmethod
    def from_file(cls, activity_json_file_path: Path) -> "Activity":
        """Load activity from file"""
        _match = cls.match_activity_file_name(activity_json_file_path.name)
        _type, _creat_date, _id, _retries = _match.groups()
        _state = activity_json_file_path.parent.name
        _data = activity_json_file_path.read_text(encoding="utf-8")
        return Activity(
            _type,
            _creat_date,
            _id,
            int(_retries),
            _state,
            _data,
        )

    @property
    def unique_key(self) -> str:
        """Returns the activity's unique key"""
        return f"activity.{self.activity_type}.{self.creation_time.strftime(self.CREATION_TIME_FORMAT)}.{self.activity_id}"

    def file_name(self) -> str:
        """Returns the unique file name for this activity"""
        return self.unique_key + f"_{self.retries}.json"

    def write_file(self, root_dir: Path) -> Path:
        """Writes activity to file; returns path of written file"""
        LOG.info("Writing new activity %s to file", self.activity_id)
        target_dir = root_dir / self.state.name
        ensure_dir_exists(target_dir)
        target_file = target_dir / self.file_name()
        if target_file.exists():
            raise FileExistsError(target_file)
        target_file.write_text(self.data, encoding="utf-8")
        return target_file

    def __str__(self) -> str:
        """Returns a string representation of the activity"""
        return f"{{id:{self.activity_id} type:{self.activity_type} creation_time:{self.creation_time} retries:{self.retries}}}"
