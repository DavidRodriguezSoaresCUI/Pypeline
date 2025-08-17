import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from time import time
from typing import Callable, Self

from drslib.path_tools import ensure_dir_exists, find_available_path

from .utils import PYPELINE_LOGGER, Singleton, random_base32

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
    IGNORED = auto()
    """Activity was marked to not be processed"""


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
    """Used to know if the activity files may be discarded"""
    retry_delay_s: int = 0
    """On retry, allows to set a delay before it can be processed again"""

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
    def retry(
        reason: str, actual_work_was_done: bool = False, retry_delay_s: int = 0
    ) -> "ExitState":
        """Create a new ExitState for typical process executions that encountered an error but can try again later"""
        return ExitState(
            ExitStatus.ERROR_RETRY,
            reason,
            ActivityState.TO_BE_PROCESSED,
            actual_work_was_done,
            retry_delay_s,
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
            + (f"retry_delay_s:{self.retry_delay_s}" if self.retry_delay_s > 0 else "")
            + "}"
        )


class Activity:
    """
    Each activity is stored as a file in a reserved location:

    - path: ``<rootDir>/<state>/activity.<type>.<creation_time>.<id>_<retry>.json`` (name example: `activity.RSSReadActivity.2024-01-01T16-30.JFU_0`)
    - attributes:

        - id (alphadecimal)
        - type (to match processor)
        - creation time (format 'YYYY-MM-DDTHH-mm')
        - state
        - input objects (content of the file)

    - Only exist as an abstraction for the input data to a processing step
    - Can conceptually be sorted into 3 groups:

        - bootstrap activities: no or little input data, and associated processor's main
        job is to (conditionally) create further activities
        - step activities: do actual work with input objects, optionally create further
        activities (including bootstrap and terminal)
        - terminal activity: like step activities but never create further activities

    While an activity file's contents are assumed to be WORM data, the file representation
    allows for easily changing activities' state by moving the associated file, creation
    by creating a new file, removal by deleting the file, etc. Only a user should edit
    an activity file's contents, and then only in exceptional cases.

    Activities are designed to represent an atomic workload, that is either fully completed
    or not at all. Writing Processors that may only partially complete the given workload
    is discouraged because of the ambiguous exit state, and may need to adopt strategies,
    like marking the partially successful activity as PROCESSED while creating a new activity
    containing the subset of failed work items for retry or failure.

    TBD: are there any reasons to add additional info, like execution context, creation
    date, history, creation processor, owner (user), whether it is part of a recurrent
    or "on demand" process, etc ?
    """

    activity_type: str
    """Used to match the activity with an appropriate processor"""
    creation_time: datetime
    """Creation date time (precision: minute, represented as YYYY-MM-DDTHH-mm)"""
    activity_id: str
    """Unique identifier and "short name" for the activity"""
    retries: int
    """Number of retries for this activity (starts at 0)"""
    retry_time: datetime | None
    """Time for next retry (precision: second, represented as YYYY-MM-DDTHH-mm-ss)"""
    state: ActivityState
    """State of the activity, represented by containing directory name"""
    data: str
    """JSON-encoded data"""

    FILE_NAME_PATTERN = re.compile(
        r"activity\.([^\.]+?)\.([0-9T-]+?)\.([^\.]+)_(\d+)(?:\.([0-9T-]+?))?\.json"
    )  # <type:str>, <create-datetime:str>, <id:str>, <retry:int>, <retry-datetime:str>
    CREATION_TIME_FORMAT = "%Y-%m-%dT%H-%M"
    RETRY_TIME_FORMAT = "%Y-%m-%dT%H-%M-%S"

    def __init__(
        self,
        activity_type: str,
        creation_time: datetime | str,
        activity_id: str,
        retries: int,
        retry_time: datetime | None,
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
        self.retry_time = retry_time
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
    def get_retry_time(cls, activity_file_name: str) -> datetime | None:
        """Get activity type from file name"""
        res = cls.match_activity_file_name(activity_file_name).group(5)
        if res is None:
            return None
        return datetime.strptime(res, cls.RETRY_TIME_FORMAT)

    @classmethod
    def from_file(cls, activity_json_file_path: Path) -> "Activity":
        """Load activity from file"""
        _match = cls.match_activity_file_name(activity_json_file_path.name)
        _type, _creat_date, _id, _retries, _retry_datetime_s = _match.groups()
        _state = activity_json_file_path.parent.name
        _data = activity_json_file_path.read_text(encoding="utf-8")
        _retry_datetime = (
            None
            if _retry_datetime_s is None
            else datetime.strptime(_retry_datetime_s, cls.RETRY_TIME_FORMAT)
        )
        return Activity(
            _type,
            _creat_date,
            _id,
            int(_retries),
            _retry_datetime,
            _state,
            _data,
        )

    @property
    def unique_key(self) -> str:
        """Returns the activity's unique key"""
        return f"activity.{self.activity_type}.{self.creation_time.strftime(self.CREATION_TIME_FORMAT)}.{self.activity_id}"

    def file_name(self, with_retry_suffix: bool = True) -> str:
        """Returns the unique file name for this activity"""
        retry_suffix = ""
        if with_retry_suffix and self.retry_time is not None:
            retry_suffix = f".{self.retry_time.strftime(self.RETRY_TIME_FORMAT)}"
        return self.unique_key + f"_{self.retries}{retry_suffix}.json"

    def write_file(self, root_dir: Path) -> Path:
        """Writes activity to file; returns path of written file"""
        LOG.info(
            "Writing new activity %s %s to file", self.activity_type, self.activity_id
        )
        target_dir = root_dir / self.state.name
        ensure_dir_exists(target_dir)
        target_file = target_dir / self.file_name()
        if target_file.exists():
            raise FileExistsError(target_file)
        target_file.write_text(self.data, encoding="utf-8")
        return target_file

    def __str__(self) -> str:
        """Returns a string representation of the activity"""
        return f"{{id:{self.activity_id} type:{self.activity_type} creation_time:{self.creation_time} retries:{self.retries} retry_time:{self.retry_time}}}"


class TrackedActivity:
    """An Orchestrator can track an activity but not OWN it"""

    activity_file: Path
    """path of the tracked activity"""
    activity_key: str
    """Unique key for tracked activity, needed for resynchronization"""
    state_timestamp: int
    """Timestamp at which the activity begun to be tracked under this state"""
    attached_files: list[Path]
    """Extra files (like log files) generated by the activity, that need to be moved with the activity file"""
    processing_schedule_delay_timestamp: float
    """Used for when a tracked activity may not be immediately scheduled for processing, for example to avoid tracked activities
    being scheduled multiple times due to the lag between it being scheduled for processing and the file being moved to the
    'IN_PROGRESS' directory, or to wait some time between retries"""

    def __str__(self) -> str:
        return (
            f"<TrackedActivity {id(self)} {self.activity_file=} {self.attached_files=}>"
        )

    def __init__(self, activity_file: Path) -> None:
        self.activity_file = activity_file
        self.activity_key = Activity.from_file(activity_file).unique_key
        self.state_timestamp = int(time())
        self.attached_files = []
        self.processing_schedule_delay_timestamp = 0.0
        LOG.info(
            "Starting to track activity %s %s",
            Activity.get_type(activity_file.name),
            Activity.get_id(activity_file.name),
        )

    def __state(self) -> ActivityState:
        """Return activity state (no resynchronisation)"""
        return ActivityState[self.activity_file.parent.name]

    @property
    def state(self) -> ActivityState:
        """Return tracked activity state"""
        self.__resynchronise()
        return self.__state()

    @property
    def type(self) -> str:
        """Return tracked activity type"""
        return Activity.get_type(self.activity_file.name)

    @property
    def activity_id(self) -> str:
        """Return tracked activity state"""
        return Activity.get_id(self.activity_file.name)

    def __retry_count(self) -> int:
        """Return activity retry count (no resynchronisation)"""
        return Activity.get_retry_count(self.activity_file.name)

    @property
    def retry_count(self) -> int:
        """Return the retry count for current activity"""
        self.__resynchronise()
        return self.__retry_count()

    @property
    def still_exists(self) -> int:
        """Return whether activity exists, False if it was removed"""
        try:
            self.__resynchronise()
        except FileNotFoundError:
            return False
        return True

    @property
    def already_scheduled_for_processing(self) -> bool:
        """Returns True of the tracked activity has been schedules for processing in the last 10s to avoir an activity being scheduled multiple times
        due to the lag between it being scheduled for processing and the file being moved to the 'IN_PROGRESS' directory. Also returns True
        on an activity scheduled for later retry.
        """
        self.__resynchronise()
        retry_time = Activity.get_retry_time(self.activity_file.name)
        return (
            self.processing_schedule_delay_timestamp > 0.0
            and self.processing_schedule_delay_timestamp > time()
        ) or (retry_time is not None and retry_time > datetime.now())

    def __resynchronise(self) -> None:
        """Resynchronises with activity files"""
        if self.activity_file.is_file():
            return

        LOG.info("Resynchronising activity %s", self.activity_file.name)
        last_known_state, last_retry_count = self.__state(), self.__retry_count()
        activity_root_dir = self.activity_file.parent.parent

        # Find lost activity
        candidate_activities = list(
            activity_root_dir.rglob(self.activity_key + "*.json")
        )
        if len(candidate_activities) == 0:
            raise FileNotFoundError(f"Lost track of activity {self.activity_id}")
        self.activity_file = max(
            candidate_activities, key=lambda _path: Activity.get_retry_count(_path.name)
        )

        # Reattach lost attached files
        for f in activity_root_dir.rglob(self.activity_key + "_*.*"):
            if f.suffix.lower() == ".json":
                continue
            if f not in self.attached_files:
                LOG.debug("Reattached file %s to activity %s", f, self.activity_id)
                self.attached_files.append(f)

        # Update state timestamp if necessary
        if self.__state() != last_known_state:
            _now = int(time())
            change = (
                f"change of state ({last_known_state} -> {self.__state()})"
                if self.__retry_count() == last_retry_count
                else f"retry ({last_retry_count} -> {self.__retry_count()})"
            )
            LOG.info(
                "Activity tracking: resynchronised with activity %s after %s (time elapsed since last change: %ss)",
                self.activity_id,
                change,
                _now - self.state_timestamp,
            )
            self.state_timestamp = _now

    def mark_as_scheduled_for_processing(self) -> None:
        """Makes the tracked activity unable to be sceduled for processing again for 10s to avoir an activity being scheduled multiple times
        due to the lag between it being scheduled for processing and the file being moved to the 'IN_PROGRESS' directory
        """
        self.processing_schedule_delay_timestamp = time() + 10.0

    def set_retry(self, retry_delay_s: int | str) -> None:
        """Increment retry count and rename file accordingly"""
        self.__resynchronise()
        activity = Activity.from_file(self.activity_file)
        activity.retries += 1

        self.state_timestamp = int(time())
        # Delay implementation
        _retry_delay_s = (
            int(retry_delay_s) if isinstance(retry_delay_s, str) else retry_delay_s
        )
        if _retry_delay_s > 0:
            self.processing_schedule_delay_timestamp = (
                self.state_timestamp + _retry_delay_s
            )
            LOG.info("Retrying activity: delay=%ss", _retry_delay_s)
            activity.retry_time = datetime.now() + timedelta(seconds=_retry_delay_s)

        self.activity_file = self.activity_file.rename(
            self.activity_file.parent / activity.file_name()
        )

    def attach_file(self, file_stem_suffix: str, file_ext: str) -> Path:
        """Adds a file to attached files so it can be moved with the activity file"""
        if not file_ext:
            raise ValueError("Missing argument 'file_ext'")
        self.__resynchronise()

        # Resolve actual path of file to attach
        file_to_attach = find_available_path(
            self.activity_file.parent,
            self.activity_key + "_" + file_stem_suffix,
            file_ext=file_ext,
        )
        if file_to_attach.suffix.lower() == ".json":
            raise ValueError(
                "Unsupported file type json for attaching to activity due to confusion potential with activity file"
            )

        LOG.debug(
            "File %s attached to activity %s",
            file_to_attach.name,
            self.activity_file.name,
        )
        self.attached_files.append(file_to_attach)
        return file_to_attach

    def change_state(
        self,
        destination_state: ActivityState,
        activity_dir_resolver: Callable[[ActivityState], Path],
    ) -> Path | None:
        """Changes state of an activity by moving it (and attached files) to the corresponding directory.
        This operation requires taking ownership of this activity but may fail (return None)
        """
        self.__resynchronise()

        destination_dir = activity_dir_resolver(destination_state)
        target_file = destination_dir / self.activity_file.name
        LOG.info(
            "Moving activity %s and its %s attached files to %s",
            self.activity_file.name,
            len(self.attached_files),
            destination_dir,
        )

        try:
            self.activity_file = self.activity_file.rename(target_file)
            self.attached_files = [
                f.rename(destination_dir / f.name) for f in self.attached_files
            ]
        except FileNotFoundError:
            # ownership and state change failed
            return None

        # ownership and state change succeeded
        self.state_timestamp = int(time())
        return target_file

    def remove(self) -> None:
        """Removes activity and related file, typically the activity is in a final state but nothing was done"""
        self.__resynchronise()

        LOG.info(
            "Removing all files related to activity %s (%s)",
            self.activity_id,
            self.type,
        )
        for _file in [self.activity_file] + self.attached_files:
            _file.unlink(missing_ok=True)


class ActivityCreator(metaclass=Singleton):
    """Responsible for all activity creation; Must be instantiated by the Orchestrator"""

    activity_root_dir: Path
    """Activities are written to subdirectories"""
    processor_name_by_handled_activity_type: dict[str, str]
    """Used to know the scope of handled activity types"""
    allowed_activity_type_to_create_by_processor_name: dict[str, set[str]]
    """Used to enforce activity creation restrictions"""

    def __init__(
        self,
        activity_root_dir: Path,
        processor_name_by_handled_activity_type: dict[str, str],
        allowed_activity_type_to_create_by_processor_name: dict[str, set[str]],
    ) -> None:
        self.activity_root_dir = activity_root_dir
        self.processor_name_by_handled_activity_type = (
            processor_name_by_handled_activity_type
        )
        self.allowed_activity_type_to_create_by_processor_name = (
            allowed_activity_type_to_create_by_processor_name
        )

    def create_activity(
        self,
        activity_type: str,
        activity_data: str,
        from_processor: str | None = None,
        current_time: datetime | None = None,
        reserved_ids: set[str] | None = None,
    ) -> Activity:
        """See `create_activities`"""
        return self.create_activities(
            activity_type, [activity_data], from_processor, current_time, reserved_ids
        )[0]

    def create_notification_activity(
        self, notifications: list[str], from_processor: str | None = None
    ) -> Activity:
        """See `create_activities`"""
        return self.create_activities(
            activity_type, [activity_data], from_processor, current_time, reserved_ids
        )[0]

    def create_activities(
        self,
        activity_type: str,
        activities_data: list[str],
        from_processor: str | None = None,
        current_time: datetime | None = None,
        reserved_ids: set[str] | None = None,
        start_delay_s: int = 0,
    ) -> list[Activity]:
        """Create a list of activities. This is meant for processors to call when producing new activities
        `from_processor` is required when a processor creates activities
        `current_time` may be used to save a call to datetime.now()
        `reserved_ids` is a blacklist for ensures the new activities' ids and may be used to avoid duplicate ids
        `start_delay_s` if >0, sets Activity.retry_time so created activity's processing is delayed'
        """

        if not isinstance(activities_data, list):
            raise ValueError(
                f"Expected 'activities_data' to be a list, got {type(activities_data)} ({activities_data})"
            )
        if start_delay_s < 0:
            raise ValueError(
                f"Value for parameter 'delay_s' must be >=0, got {start_delay_s}"
            )
        if current_time is None:
            current_time = datetime.now()
        _reserved_ids = set() if reserved_ids is None else set(reserved_ids)

        # If a processor tries to create an activity, it has to match the declared set of output activity types
        if (
            from_processor is not None
            and activity_type
            not in self.allowed_activity_type_to_create_by_processor_name[
                from_processor
            ]
        ):
            raise TypeError(
                f"Activity type {activity_type} is not declared in {from_processor}'s set of output activity types."
            )

        LOG.info(
            "Creating %s activities of type %s%s",
            len(activities_data),
            activity_type,
            "" if from_processor is None else f" for processor {from_processor}",
        )
        created_activities = []
        for activity_data in activities_data:
            # Ensure activity ids are unique
            activity_id: str
            while (activity_id := random_base32(3)) in _reserved_ids:
                continue
            _reserved_ids.add(activity_id)

            activity = Activity(
                activity_type=activity_type,
                creation_time=current_time,
                activity_id=activity_id,
                retries=0,
                retry_time=(
                    None
                    if start_delay_s == 0
                    else current_time + timedelta(seconds=start_delay_s)
                ),
                state=ActivityState.TO_BE_PROCESSED,
                data=activity_data,
            )
            activity.write_file(self.activity_root_dir)
            created_activities.append(activity)

        return created_activities


class ActivityData(ABC):
    """Abstract base class for any activity type's data.
    Provides the following methods:
    - `to_json`: instance method to serialize the activity data (handy when creating new activities)
    - `from_json`: class method to deserialize the activity data (necessary for reading it, see example below)

    It is highly recommended to also use the `@dataclass` annotation on any subclass, as to simplify them to field declaration.

    The following sample code is the recommended way to implement an ActivityData and Processor pair for the example activity type `SendEmailActivity` :

    activity_bootstrap.csv::

        activityType,workerID,bootstrapRule,onFirstCycle,activityData
        SendEmailActivity,ExampleWorker,@every 24h,false,"{""sender"": ""pypeline@example.com"", ""recipients"": [""alice@example.com"", ""bob@example.com""], ""message"": ""The server works today""}"

    python code::

        from dataclasses import dataclass
        import json

        @dataclass
        class SendEmailActivityData(ActivityData):
            '''Represents data input for activity SendEmailActivity'''
            sender: str
            recipients: list[str]
            message: str

        class SendEmailProcessor(Processor):
            INPUT_ACTIVITY_TYPE = "SendEmailActivity"
            OUTPUT_ACTIVITY_TYPES = set()

            @classmethod
            def execute(cls, activity: Activity, log: logging.Logger) -> ExitState:
                activity_data = SendEmailActivityData.from_json(activity.data)
                log.info("Will send email to recipients: %s", activity_data.recipients)
                send_email(activity_data.sender, activity_data.recipients, activity_data.message)
                cls.create_activities(
                    activity_type=NotifyProcessor.INPUT_ACTIVITY_TYPE,
                    activities_data=[NotifyActivityData("Emails sent!").to_json()],
                )
                return ExitState.success()

    Note that this Processor creates new activities; Implementation of NotifyActivityData/NotifyProcessor is not included but follows the same design.
    """

    @abstractmethod  # Required to disable instantiation
    def __init__(self) -> None: ...

    @classmethod
    def from_json(cls, json_content: str) -> Self:
        """Instantiate from JSON representation"""
        return cls(**json.loads(json_content))

    def to_json(self) -> str:
        """Returns a JSON representation for the current object"""
        return json.dumps(self.__dict__, ensure_ascii=False)
