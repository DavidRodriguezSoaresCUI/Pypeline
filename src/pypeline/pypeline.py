"""
the pypeline
============

The pypeline is the collective designation for the following artifacts:

- the workers
- the orchestrators (one per worker)
- the processors (across all workers)
- the activities (single repository for all workers)

Together they handle complex workflows and do actual work reliably (and in a
distributed fashion with multiple workers).


on multiple workers
===================

When multiple workers operate on the same activities repository, there are inherent
complications. Mostly, if more than one worker accepts activities of a given type,
there must be a process by which the activity is attributed to a single worker. Here
we use a naive solution based on renaming the file and checking whether that action
was successful.


the state
=========

The processors are entirely stateless and the activities are entirely stateful. The
orchestrator is stateful in the sense that it accesses a configuration (shared by all
workers) and handles activities which are stateful. Therefore, "the state of the
pypeline" refers to the shared activities and orchestrator configuration.
"""

from functools import partial
import logging
import string
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict

# from drslib.debug import debug_var
from drslib.multiprocessing import NestablePool
from drslib.path_tools import ensure_dir_exists
from drslib.stream import Collector, Stream
import yaml

from .activity import (
    Activity,
    ActivityCreator,
    ActivityState,
    ExitState,
    ExitStatus,
    TrackedActivity,
)
from .activity_bootstrap_rule import ActivityBootstrapRule
from .processor import Processor
from .properties_manager import PropertiesManager, PropertySpec, configuration_to_yaml
from .rule_engine import (
    ACTIVITY_BOOTSTRAP_META_HEADER,
    ACTIVITY_PROCESSING_CONFIG_FILE_NAME,
    ACTIVITY_PROCESSING_CONFIG_HEADER,
    ACTIVITY_PROCESSING_META_HEADER,
    BOOTSTRAP_CONFIG_FILE_NAME,
    BOOTSTRAP_CONFIG_HEADER,
    LABEL_ACTIVITY_DATA,
    LABEL_ACTIVITY_TYPE,
    LABEL_BOOTSTRAP_RULE,
    LABEL_FIRE_ON_FIRST_CYCLE,
    LABEL_PARELLEL_PROCESSES,
    LABEL_WORKER_ID,
    NoRuleMatchError,
    RuleEngine,
    RuleEngineTypes,
)
from .utils import (
    LOG_FORMAT_WITH_TIME,
    LOG_SEPARATOR,
    PYPELINE_LOGGER,
    FileDefinedValue,
    OrchestratorReturnCode,
    Singleton,
    add_file_handler,
    change_formatters,
    full_exception,
    make_string_json_safe,
    remove_file_handlers,
)

LOG = logging.getLogger(PYPELINE_LOGGER)

PROCESSOR_LOG_FORMAT = "[%(asctime)s][%(levelname)s][{}] %(message)s"
ALLOWED_WORKER_ID_CHARACTERS = set(string.ascii_letters + string.digits + "-_")


def process_activity(
    tracked_activity: TrackedActivity,
    activity_dirs: Dict[ActivityState, Path],
    processor: Processor,
    worker_id: str,
    properties_manager_setup: list[Path],
    processor_config: dict,
) -> Exception | None:
    """Called in a subprocess to run a Processor on an Activity"""

    # Set up PropertiesManager if necessary
    if PropertiesManager.get_instance() is None:
        LOG.info("Setting properties manager in processor execution context")
        PropertiesManager(properties_manager_setup)

    # Set up some variables
    start_datetime = datetime.fromtimestamp(int(time.time()))
    start_datetime_readable = start_datetime.isoformat()
    activity_id = tracked_activity.activity_id
    activity_type = tracked_activity.type

    # Change activity status to IN_PROGRESS
    activity_status_dir_resolver: Callable[
        [ActivityState], Path
    ] = lambda activity_state: activity_dirs[activity_state]
    in_progress_activity_file = tracked_activity.change_state(
        ActivityState.IN_PROGRESS, activity_status_dir_resolver
    )
    if in_progress_activity_file is None:
        LOG.error(
            "Failed to transition activity %s to state IN_PROGESS; aborting",
            activity_id,
        )
        return None

    # Read activity from file
    activity = Activity.from_file(in_progress_activity_file)

    # Set up logger for this execution (logs to dedicated file only)
    logfile = tracked_activity.attach_file(
        start_datetime_readable.replace(":", "-"), file_ext=".log"
    )
    log = logging.getLogger(activity_type + activity_id)
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    add_file_handler(log, logfile, logging.DEBUG)
    log.debug("Logger '%s' is now active", activity_type + activity_id)

    proc_name = processor.__qualname__  # type: ignore[attr-defined]
    log.info(
        "Start processing activity %s (%s) at %s using processor %s",
        activity_id,
        activity_type,
        start_datetime_readable,
        proc_name,
    )

    # Some last-second setup
    log.info(LOG_SEPARATOR)
    change_formatters(log, PROCESSOR_LOG_FORMAT.format(proc_name))
    processor.set_property_prefix(worker_id)

    # Processor exection and exception handling
    proc_ex: Exception | None = None
    try:
        exit_state = processor.execute(activity, log, processor_config)
    except Exception as e:
        log.error("Processor execution was interrupted by the following exception")
        log.exception(e)
        exit_state = ExitState.error(str(e))
        proc_ex = e

    # Post-execution actions
    end_datetime = datetime.fromtimestamp(int(time.time()))
    change_formatters(log, LOG_FORMAT_WITH_TIME)
    log.info(LOG_SEPARATOR)
    if exit_state is None:
        raise ValueError(f"Processor {proc_name} did not return an exit state")

    # Post-execution logs
    log.info(
        "End processing activity at %s (time elapsed: %s)",
        end_datetime.isoformat(),
        end_datetime - start_datetime,
    )
    status_logger = log.info if exit_state.status is ExitStatus.SUCCESS else log.warning
    status_logger("Execution of activity ended with status %s", exit_state)

    # Remove file handler so log files can be moved
    remove_file_handlers(log)

    if exit_state.remove_activity:
        tracked_activity.remove()
    else:
        if exit_state.status == ExitStatus.ERROR_RETRY:
            tracked_activity.set_retry(exit_state.retry_delay_s)
        tracked_activity.change_state(
            exit_state.next_activity_status, activity_status_dir_resolver
        )
    LOG.info("Post-execution actions done. The process will exit now")

    return proc_ex


class Orchestrator(metaclass=Singleton):
    """
    Each "worker" runs a single instance of the orchestrator, that :

    - has access to a set of processors
    - can see the activities
    - executes processors on activities
    - makes the status of activities progress (by moving them)
    - configuration in a table file with rules for:

      - bootstrap activity creation
      - activity retention time
      - activity lock retention time

    Properties:

    - `Orchestrator.process-pool-size` : [int; default 2] the number of processes available to process activities

      > **WARNING**: is only read once at initialization time

    - `Orchestrator.main-loop.stop-now` : [bool; default false] value in true/false; if true the main loop will exit.
    - `Orchestrator.main-loop.sleep-ms` : [int; default 2000] value in milliseconds, representing the amount of time to sleep between iterations of the main loop (activity bootstrapping and processing).
    - `Orchestrator.main-loop.log-cooldown-seconds`: [int; default 15] value in seconds, representing the minimum amount of time between `Orchestrator running` logs (will be logged at the next main loop iteration after cooldown expires).
    - Two properties for handling activities in error (ends with ActivityState.ERROR or raises exception) by creating a new activity to handle the situation (ex: notify, take corrective measures):

      - `Orchestrator.on-activity-error.<errored-activity-type>.handler-activity-type`: [string; must be valid activity type] specifies the activity **type** for the error handling activity to create
      - `Orchestrator.on-activity-error.<errored-activity-type>.handler-activity-data`: [string; must be valid activity data] specifies the activity **data** for the error handling activity to create
      > Remember that for `<errored-activity-type>` you can have a global rule with `*` and override it with specific rules for each activity type
    """

    root_dir: Path
    """Root directory containing configuration and activity files"""
    processor_name_by_handled_activity_type: dict[str, str]
    """Map of processors by the activity type they handle"""
    processor_by_name: dict[str, Processor]
    """Map of processors by the activity type they handle"""
    activity_processing_configuration: FileDefinedValue[RuleEngine]
    """Configuration on activity bootstrapping"""
    tracked_activities: dict[str, TrackedActivity]
    """Orchestrator keeps track of activities it wants to process or is processing"""
    worker_id: str
    """Unique identifier for this worker"""
    activity_bootstrap_rules: FileDefinedValue[list[ActivityBootstrapRule]]
    """Rules governing activities to create"""
    # timer_manager: TimerManager
    # processor_runner: ProcessorRunner
    pool: NestablePool
    properties_manager: PropertiesManager
    activity_creator: ActivityCreator
    processor_conf: FileDefinedValue[dict]

    # Properties used
    ROOT_PPTY = ["Orchestrator"]
    SLEEP_MSECONDS_PPTY = ROOT_PPTY + ["main-loop", "sleep-ms"]
    SLEEP_MSECONDS_DEFAULT = 2000
    LOOP_LOG_COOLDOWN_SECONDS_PPTY = ROOT_PPTY + ["main-loop", "log-cooldown-seconds"]
    LOOP_LOG_COOLDOWN_SECONDS_DEFAULT = 15
    STOP_NOW_PPTY = ROOT_PPTY + ["main-loop", "stop-now"]
    RELOAD_NOW_PPTY = ROOT_PPTY + ["reload-now"]
    ON_EXCEPTION_ACTIVITY_TYPE_PPTY = (
        lambda self, errored_activity_type: self.ROOT_PPTY
        + [
            "on-activity-error",
            errored_activity_type,
            "handler-activity-type",
        ]
    )
    ON_EXCEPTION_ACTIVITY_CONTENT_PPTY = (
        lambda self, errored_activity_type: self.ROOT_PPTY
        + [
            "on-activity-error",
            errored_activity_type,
            "handler-activity-data",
        ]
    )
    PROCESS_POOL_SIZE_PPTY = ROOT_PPTY + ["process-pool-size"]
    PROCESS_POOL_SIZE_DEFAULT = 2
    ORCHESTRATOR_PROPERTIES = [
        PropertySpec(
            RELOAD_NOW_PPTY,
            "bool",
            "Aimed at developers for reloading the application after changes. Set to true to stop the program with return code 2. Immediate reload requires using this value to reload. Warning: will be commented before reload to avoid a reload loop.",
            default=False,
        ),
        PropertySpec(
            STOP_NOW_PPTY,
            "bool",
            "Set to true to stop the program (return code 0)",
            default=False,
        ),
        PropertySpec(
            SLEEP_MSECONDS_PPTY,
            "int",
            "Time in seconds, wait time between main loop iterations",
            default=SLEEP_MSECONDS_DEFAULT,
        ),
        PropertySpec(
            LOOP_LOG_COOLDOWN_SECONDS_PPTY,
            "int",
            "Time in seconds, minimum time between two main loop iteration 'Orchestrator run' logs (set higher to reduce idle logging)",
            default=LOOP_LOG_COOLDOWN_SECONDS_DEFAULT,
        ),
        PropertySpec(
            PROCESS_POOL_SIZE_PPTY,
            "int",
            "Number of worker threads",
            default=PROCESS_POOL_SIZE_DEFAULT,
        ),
    ]

    def __init__(
        self, processors: list[Processor], root_dir: Path, worker_id: str
    ) -> None:
        self.root_dir = root_dir
        ensure_dir_exists(root_dir)

        property_files = list(self.root_dir.glob("*.properties"))
        if not property_files:
            property_files = [self.root_dir / "default.properties"]
        self.properties_manager = PropertiesManager(property_files)
        self.properties_manager.insert_processor_sections_if_not_exist(
            self.__class__, self.ORCHESTRATOR_PROPERTIES
        )

        self.processor_conf = FileDefinedValue(
            self.root_dir / "processor.conf.yaml",
            lambda p: yaml.safe_load(p.read_text(encoding="utf8")),
        )

        self.worker_id = worker_id
        if len(worker_id) < 3:
            raise ValueError(
                "worker_id too short; please make it at lease 3 characters"
            )
        illegal_characters = set(
            c for c in worker_id if c not in ALLOWED_WORKER_ID_CHARACTERS
        )
        if illegal_characters:
            raise ValueError(
                f"Illegal characters found in worker ID '{worker_id}': {illegal_characters}"
            )

        add_file_handler(LOG, root_dir / f"worker.{worker_id}.log")

        # Validate processors and insert properties/configuration items if needed
        Orchestrator.validate_processors(
            processors, self.properties_manager, self.processor_conf
        )

        self.processor_name_by_handled_activity_type = {
            p.get_input_activity_type(): p.__name__ for p in processors
        }
        self.processor_by_name = {p.__name__: p for p in processors}

        self.activity_creator = ActivityCreator(
            root_dir,
            self.processor_name_by_handled_activity_type,
            {p.__name__: p.get_output_activity_types() for p in processors},
        )

        self.tracked_activities = {}
        self.activity_bootstrap_rules = FileDefinedValue[list[ActivityBootstrapRule]](
            self.root_dir / BOOTSTRAP_CONFIG_FILE_NAME, self.setup_bootstraps()
        )
        if worker_id is None:
            LOG.warning("Worker id is not set")

        self.activity_processing_configuration = FileDefinedValue[RuleEngine](
            root_dir / ACTIVITY_PROCESSING_CONFIG_FILE_NAME,
            self.setup_activity_processing_configuration(),
        )

        self.pool = NestablePool(
            max(
                1,
                self.properties_manager.get_int(
                    self, self.PROCESS_POOL_SIZE_PPTY, self.PROCESS_POOL_SIZE_DEFAULT
                ),
            )
        )

    @staticmethod
    def validate_processors(
        processors, pm: PropertiesManager, processor_conf: FileDefinedValue[dict]
    ):
        """Runs processor self-validation, then writes declared properties/configuration in relevant files if necessary"""
        for p in processors:
            p.validate()
            p.set_property_prefix("*")  # required for resolving properties
            declared_properties = p.get_properties()
            if declared_properties:
                LOG.info("Found declared properties for processor %s", p.__name__)
                pm.insert_processor_sections_if_not_exist(p, declared_properties)
            declared_configuration = p.get_configuration()
            if declared_configuration:
                LOG.info("Found declared configuration for processor %s", p.__name__)
                if p.get_input_activity_type() not in processor_conf.get():
                    LOG.info(
                        "Will add missing processor configuration: file=%s processor=%s",
                        processor_conf.source_file,
                        p.__name__,
                    )
                    processor_block = configuration_to_yaml(
                        declared_configuration, parent=p.get_input_activity_type()
                    )
                    processor_conf.edit_content(
                        lambda conf: conf + "\n\n" + processor_block
                    )

    @property
    def handled_activity_types(self) -> set[str]:
        """Returns a copy of handled activity types set"""
        return set(self.processor_name_by_handled_activity_type.keys())

    def activity_dir(self, activity_state: ActivityState) -> Path:
        """Returns path of directory containing activities of corresponding state"""
        _path = self.root_dir / activity_state.name
        ensure_dir_exists(_path)
        return _path

    @property
    def activity_dirs(self) -> Dict[ActivityState, Path]:
        """Returns all activity directories by state"""
        return {_as: self.activity_dir(_as) for _as in ActivityState}

    def get_untracked_activities(
        self, selected_state: ActivityState
    ) -> dict[str, Path]:
        """Returns untracked activities of selected state"""
        return {
            activity_id: f
            for f in self.activity_dir(selected_state).glob("activity.*.json")
            if (activity_id := Activity.get_id(f.name)) not in self.tracked_activities
            # or self.tracked_activities[activity_id].state is not selected_state # TODO: I commented this line because I couldn't understand the case
        }

    def get_tracked_activities(
        self, selected_state: ActivityState
    ) -> list[TrackedActivity]:
        """Returns tracked activities of selected state"""
        res = []
        for k, t_a in dict(self.tracked_activities).items():
            try:
                if t_a.state is selected_state:
                    res.append(t_a)
            except FileNotFoundError:
                del self.tracked_activities[k]
        return res

    def allowed_parallel_processes(self, activity_type: str) -> int:
        """Fetches allowed parallel processes from configuration"""
        _criteria: dict[str, RuleEngineTypes] = {LABEL_ACTIVITY_TYPE: activity_type}
        if self.worker_id is not None:
            _criteria[LABEL_WORKER_ID] = self.worker_id
        conf: RuleEngine = self.activity_processing_configuration.get()
        res = conf.get_single_mapping(
            criteria=_criteria,
            value=LABEL_PARELLEL_PROCESSES,
        )
        if isinstance(res, int):
            return res
        raise ValueError(
            f"Expected int value, found {type(res)} when fetching value for {LABEL_PARELLEL_PROCESSES} from {conf.configuration_file.stem} with criteria {_criteria}"
        )

    def process_tbp_activities(self) -> None:
        """Refresh the list of activities that may be processed next and
        kickstart their execution
        """

        # find new TBP activities
        _new_activities = self.get_untracked_activities(ActivityState.TO_BE_PROCESSED)
        if _new_activities:
            LOG.info("Found new activities: %s", _new_activities)
        for activity_id, activity_file in _new_activities.items():
            _activity = TrackedActivity(activity_file)
            if _activity.type not in self.handled_activity_types:
                LOG.warning(
                    "Found activity of unhandled type %s: skipping", _activity.type
                )
            else:
                self.tracked_activities[activity_id] = _activity

        tbp_activity_files_by_type = (
            Stream(self.get_tracked_activities(ActivityState.TO_BE_PROCESSED))
            .filter(lambda ta: not ta.already_scheduled_for_processing)
            .collect(
                Collector.to_defaultdict(
                    key_mapper=lambda ta: ta.type,
                    value_mapper=lambda ta: ta.activity_file,
                )
            )
        )

        for _type, _activity_files in tbp_activity_files_by_type.items():
            # Get available execution parallel instances # TODO: recheck
            available_execution_threads = self.allowed_parallel_processes(_type) - sum(
                1
                for x in self.get_tracked_activities(ActivityState.IN_PROGRESS)
                if x.type == _type
            )

            if available_execution_threads <= 0:
                continue

            # A subset of activities that can be launched immediately is selected ..
            will_process_now_count = min(
                len(_activity_files), available_execution_threads
            )

            # .. and its execution is launched
            LOG.info(
                "Starting the processing of %s activities of type %s",
                will_process_now_count,
                _type,
            )
            _proc_name = self.processor_name_by_handled_activity_type[_type]
            _processor = self.processor_by_name[_proc_name]

            # Fetch processor configuration
            _processor_config = self.processor_conf.get().get(_type, {})
            if not _processor_config:
                _processor_config = self.processor_conf.get().get(_proc_name, {})

            _on_activity_processing_error = partial(
                self.on_activity_processing_error, _type
            )
            for activity_file in _activity_files[:will_process_now_count]:
                tracked_activity = self.tracked_activities[
                    Activity.get_id(activity_file.name)
                ]
                tracked_activity.mark_as_scheduled_for_processing()
                self.pool.apply_async(
                    process_activity,
                    kwds={
                        "processor": _processor,
                        "activity_dirs": self.activity_dirs,
                        "tracked_activity": tracked_activity,
                        "worker_id": self.worker_id,
                        "properties_manager_setup": self.properties_manager.source_files,
                        "processor_config": _processor_config,
                    },
                    callback=_on_activity_processing_error,
                    error_callback=_on_activity_processing_error,
                )

    def on_activity_processing_error(
        self, errored_activity_type: str, exception: Exception
    ) -> None:
        """If setup correctly, caught processor runtime exceptions can be processed
        by creating new activities.
        Available macros are $ERROR_MSG, $FAILED_PROC
        """
        if exception is None:
            return

        exception_msg = full_exception(exception)

        on_exception_activity_content = self.properties_manager.get_string(
            self, self.ON_EXCEPTION_ACTIVITY_CONTENT_PPTY(errored_activity_type)
        )

        if on_exception_activity_content is None:
            exception_properties = [
                self.properties_manager.resolve_property_name(self, ppty)
                for ppty in (
                    self.ON_EXCEPTION_ACTIVITY_TYPE_PPTY(errored_activity_type),
                    self.ON_EXCEPTION_ACTIVITY_CONTENT_PPTY(errored_activity_type),
                )
            ]
            LOG.warning(
                "No activity set up to process caught exception. Set up by setting properties '%s' and '%s'. Exception :\n%s",
                exception_properties[0],
                exception_properties[1],
                exception_msg,
            )
        else:
            on_exception_activity_type = self.properties_manager.get_string(
                self, self.ON_EXCEPTION_ACTIVITY_TYPE_PPTY(errored_activity_type)
            )
            if on_exception_activity_type not in self.handled_activity_types:
                LOG.warning(
                    "Processor %s raised an exception but exception processing is not set up",
                    self.processor_name_by_handled_activity_type[errored_activity_type],
                )
                return

            self.activity_creator.create_activity(
                activity_type=on_exception_activity_type,
                activity_data=on_exception_activity_content.replace(
                    "$ERROR_MSG", make_string_json_safe(exception_msg)
                ).replace(
                    "$FAILED_PROC",
                    self.processor_name_by_handled_activity_type[errored_activity_type],
                ),
                reserved_ids=set(self.tracked_activities.keys()),
            )

    def setup_bootstraps(self) -> Callable[[Path], list[ActivityBootstrapRule]]:
        """Given bootstrap configuration, create activities in TO_BE_PROCESSED status"""

        _handled_activity_types = self.handled_activity_types
        _worker_id = self.worker_id

        def inner(configuration_file: Path) -> list[ActivityBootstrapRule]:
            bootstrap_configuration = RuleEngine(
                configuration_file,
                columns=BOOTSTRAP_CONFIG_HEADER,
                meta_header=ACTIVITY_BOOTSTRAP_META_HEADER,
            )

            def build_criteria(activity_type: str) -> dict:
                res = {LABEL_ACTIVITY_TYPE: activity_type}
                if _worker_id:
                    res[LABEL_WORKER_ID] = _worker_id
                return res

            # <activity_type>: [(<bootstrap_rule>, <activity_data>, <do_fire_on_first_cycle>)]
            bootstrap_data_by_type: dict[str, list[tuple[str, str, bool]]] = {}
            for _type in _handled_activity_types:
                try:
                    bootstrap_data_by_type[_type] = [
                        (
                            str(mapping[LABEL_BOOTSTRAP_RULE]),
                            str(mapping[LABEL_ACTIVITY_DATA]),
                            bool(mapping[LABEL_FIRE_ON_FIRST_CYCLE]),
                        )
                        for mapping in bootstrap_configuration.get_mappings(
                            build_criteria(_type),
                            values=[
                                LABEL_BOOTSTRAP_RULE,
                                LABEL_ACTIVITY_DATA,
                                LABEL_FIRE_ON_FIRST_CYCLE,
                            ],
                        )
                    ]
                except NoRuleMatchError:
                    LOG.info("Activity type %s has no bootstrap rule", _type)

            LOG.info(
                "Found bootstrap rule(s) for activity type(s) %s",
                ",".join(bootstrap_data_by_type.keys()),
            )
            activity_bootstrap_rules: list[ActivityBootstrapRule] = []
            for (
                _type,
                _bootstrap_rules,
            ) in bootstrap_data_by_type.items():
                for _bootstrap_data in _bootstrap_rules:
                    _bootstrap_rule, _activity_data, _fire_on_first_cycle = (
                        _bootstrap_data[0],
                        _bootstrap_data[1].replace(
                            "$PYPELINE_DIR", self.root_dir.as_posix()
                        ),
                        _bootstrap_data[2],
                    )
                    if not isinstance(_bootstrap_rule, str):
                        raise ValueError(
                            f"Expected bootstrapping rule to be a string, found {_bootstrap_rule} ({type(_bootstrap_rule)})"
                        )
                    activity_bootstrap_rules.append(
                        ActivityBootstrapRule(
                            _type, _bootstrap_rule, _activity_data, _fire_on_first_cycle
                        )
                    )

            return activity_bootstrap_rules

        return inner

    def setup_activity_processing_configuration(self) -> Callable[[Path], RuleEngine]:
        """Reads the activity processing configuration file and verifies all activities have a row"""

        _handled_activity_types = self.handled_activity_types

        def inner(configuration_file: Path) -> RuleEngine:
            rule_engine = RuleEngine(
                configuration_file=configuration_file,
                columns=ACTIVITY_PROCESSING_CONFIG_HEADER,
                meta_header=ACTIVITY_PROCESSING_META_HEADER,
            )

            for activity_type in _handled_activity_types:
                try:
                    rule_engine.get_single_mapping(
                        {LABEL_ACTIVITY_TYPE: activity_type}, LABEL_WORKER_ID
                    )
                except NoRuleMatchError as e:
                    raise NoRuleMatchError(
                        f"No rule in file {configuration_file} for activity {activity_type}"
                    ) from e

            return rule_engine

        return inner

    def do_bootstrap_activities(self, current_time: datetime) -> None:
        """Executes bootstrap rules at current time"""
        for _rule in self.activity_bootstrap_rules.get():
            try:
                new_activity = _rule.apply(current_time, self.tracked_activities.keys())  # type: ignore[arg-type]
                if new_activity is not None:
                    LOG.debug("rule=%s new_activity=%s", _rule, new_activity)
            except Exception as e:
                LOG.exception(e)

    def resynchronize_tracked_activities(self) -> None:
        """Removes deleted items from tracked_activities"""
        self.tracked_activities = {
            ta_id: ta
            for ta_id, ta in self.tracked_activities.items()
            if ta.still_exists
        }
        if len(self.tracked_activities) > 2**10:
            LOG.warning(
                "Unusually high tracked activity count: %s",
                len(self.tracked_activities),
            )

    def run(self) -> int:
        """This is the main loop"""

        pm: PropertiesManager = self.properties_manager
        last_log = 0.0

        try:
            while True:
                current_time = datetime.now()

                # Log that the main loop is alive and running
                log_cooldown_s = pm.get_int(
                    self,
                    self.LOOP_LOG_COOLDOWN_SECONDS_PPTY,
                    self.LOOP_LOG_COOLDOWN_SECONDS_DEFAULT,
                )
                if current_time.timestamp() - last_log > log_cooldown_s:
                    last_log = current_time.timestamp()
                    LOG.info("Orchestrator run")

                # Exit main loop on property
                if pm.get_bool(self, self.RELOAD_NOW_PPTY, default=False):
                    pm.comment_property(self, self.RELOAD_NOW_PPTY)
                    return OrchestratorReturnCode.RELOAD.value
                if pm.get_bool(self, self.STOP_NOW_PPTY, default=False):
                    return OrchestratorReturnCode.EXIT_OK.value

                # bootstrap activities
                self.do_bootstrap_activities(current_time)

                # process activities
                try:
                    self.process_tbp_activities()
                except Exception as e:
                    LOG.exception(e)

                self.resynchronize_tracked_activities()

                # sleep until next cycle
                sleep_time_s = (
                    pm.get_int(
                        self,
                        self.SLEEP_MSECONDS_PPTY,
                        default=self.SLEEP_MSECONDS_DEFAULT,
                    )
                    / 1_000
                )
                time.sleep(sleep_time_s)
        finally:
            LOG.info("Shutting down now")
            self.pool.close()
