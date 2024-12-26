import json
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# necessary to solve cyclic import issue
from pypeline.properties_manager import PropertiesManager
import pypeline.pypeline

from .activity import (
    VALID_ACTIVITY_TYPE_PATTERN,
    VALID_ACTIVITY_TYPE_PATTERN_HELP,
    Activity,
    ExitState,
)


class Processor:
    """
    Each processor :

    - must at least implement :
        - class constant INPUT_ACTIVITY_TYPE: str
        - class constant OUTPUT_ACTIVITY_TYPES: set[str]
        - class method ``execute(cls, activity: Activity, log: logging.Logger) -> ExitState``
    - contains the logic required to process a specific type of activity
    - is implemented as a subclass of the base Processor class
    - is passed to the orchestrator, thus enabling it to process specific activity types
    - only contain class method and attributes => stateless by design
    - has access to a properties manager with `PropertiesManager.get_instance()`
      (tip: use `cls` as value for methods' parameter `obj`)
    - may create further activities using provided method `create_activities`
    - logs are produced in a file named ``<activityFileStem>.<processingStartTimestamp>.log``
      with the timestamp being a ISO date with seconds-level precision
    """

    __property_prefix__: str
    """Makes it easy to use a properties manager"""

    @classmethod
    def set_property_prefix(cls, value: str) -> None:
        """Sets property prefix to make it possible for PropertyManager to get the id for this processor"""
        cls.__property_prefix__ = cls.get_input_activity_type() + "." + value

    @classmethod
    def validate(cls) -> None:
        """Executes some basic calls to check that class attributes are set"""
        _input = cls.get_input_activity_type()
        if not isinstance(_input, str):
            raise ValueError(
                f"Processor {cls.__name__} has INPUT_ACTIVITY_TYPE of type {type(_input)} instead of str"
            )
        if not VALID_ACTIVITY_TYPE_PATTERN.match(_input):
            raise ValueError(
                f"Processor {cls.__name__} has invalid INPUT_ACTIVITY_TYPE '{_input}': {VALID_ACTIVITY_TYPE_PATTERN_HELP}"
            )
        _outputs = cls.get_output_activity_types()
        if not isinstance(_outputs, set):
            raise ValueError(
                f"Processor {cls.__name__} has OUTPUT_ACTIVITY_TYPES of type {type(_outputs)} instead of list"
            )
        _outputs_disallowed_item_type = {type(x) for x in _outputs}.difference({str})
        if _outputs_disallowed_item_type:
            raise ValueError(
                f"Processor {cls.__name__} has OUTPUT_ACTIVITY_TYPES containing items of type {_outputs_disallowed_item_type} instead of only str"
            )

    @classmethod
    def get_input_activity_type(cls) -> str:
        """Declares which activity type the processor accepts"""
        res = getattr(cls, "INPUT_ACTIVITY_TYPE")
        if res is None:
            raise NotImplementedError()
        return res

    @classmethod
    def get_output_activity_types(cls) -> set[str]:
        """Declares which activity types the processor can generate"""
        res = getattr(cls, "OUTPUT_ACTIVITY_TYPES")
        if res is None:
            raise NotImplementedError()
        return res

    @classmethod
    def execute(
        cls,
        activity: Activity,
        log: logging.Logger,
    ) -> ExitState:
        """Declares which activity types the processor can generate"""
        raise NotImplementedError()

    @classmethod
    def execute_outer(cls, activity: Activity, log: logging.Logger) -> ExitState:
        """Wrapper around execute to catch exceptions"""
        if cls.__property_prefix__ is None:
            raise ValueError("Property prefix is not set")
        try:
            return cls.execute(activity, log)
        except Exception as e:
            return ExitState.error(str(e))

    @classmethod
    def create_activities(cls, activity_type: str, activities_data: list[str]) -> None:
        """Calls the orchestrator which will create the activities"""
        pm: PropertiesManager = PropertiesManager.get_instance()
        activity_limit = pm.get_int(cls, "activity-creation-limit", default=10)
        if len(activities_data) > activity_limit:
            raise ValueError(
                f"Tried to exceed activity creation limits: max={activity_limit} given={len(activities_data)}"
            )
        pypeline.pypeline.Orchestrator.get_instance().create_activities(
            activity_type, activities_data, cls
        )


def get_available_processors(_globals: dict) -> list[type]:
    """Useful for callers of the Orchestrator constructor"""
    return [
        v
        for v in _globals.values()
        if isinstance(v, type) and issubclass(v, Processor) and v is not Processor
    ]


@dataclass
class ActivityArchivalActivity:
    """Dumb class, needs to be JSON serializable"""

    processed_activities_path: str
    """Where processed activities are located"""
    archive_dir_path: str
    """Where to put archives"""

    def to_json(self) -> str:
        """Returns a JSON representation for the current object"""
        return json.dumps(self.__dict__, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_content: str) -> "ActivityArchivalActivity":
        """Deserialize object from JSON representation"""
        obj = json.loads(json_content)
        return ActivityArchivalActivity(
            obj["processed_activities_path"], obj["archive_dir_path"]
        )


class ActivityArchivalProcessor(Processor):
    """Uses a bot to post messages on Telegram
    Expects activity to contain a serialized TelegramMessages object
    """

    INPUT_ACTIVITY_TYPE = "ArchiveActivities"
    OUTPUT_ACTIVITY_TYPES: set[str] = set()

    @classmethod
    def execute(cls, activity: Activity, log: logging.Logger) -> ExitState:
        """Reads TelegramMessages from activity and post messages to target chat as bot"""
        activity_data = ActivityArchivalActivity.from_json(activity.data)

        processed_activities_path = Path(activity_data.processed_activities_path)
        if (
            not processed_activities_path.exists()
            or not processed_activities_path.is_dir()
        ):
            log.error(
                "Invalid path given for processed activities directory '%s'",
                processed_activities_path,
            )
            return ExitState.error(f"Invalid path '{processed_activities_path}'")
        archive_dir_path = Path(activity_data.archive_dir_path)
        if not archive_dir_path.exists():
            archive_dir_path.mkdir()
        elif not archive_dir_path.is_dir():
            log.error(
                "Invalid path given for archived activities directory '%s'",
                archive_dir_path,
            )
            return ExitState.error(f"Invalid path '{archive_dir_path}'")

        processed_activity_files = list(processed_activities_path.glob("activity.*"))
        if not processed_activity_files:
            log.info("No activity found in '%s'", processed_activities_path)
            return ExitState.success("Nothing to do", actual_work_was_done=False)

        timestamp = datetime.now().isoformat().replace(":", "-")
        archive_path = archive_dir_path / f"activities.{timestamp}.zip"
        log.info(
            "Writing %s files to archive file '%s'",
            len(processed_activity_files),
            archive_path,
        )

        with zipfile.ZipFile(
            archive_path, mode="w", compression=zipfile.ZIP_BZIP2, compresslevel=4
        ) as archive:
            for _file in processed_activity_files:
                archive.write(_file)
                _file.unlink()

        return ExitState.success()
