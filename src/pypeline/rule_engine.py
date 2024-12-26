import logging
import re
from collections import namedtuple
from csv import DictReader
from pathlib import Path
from typing import Callable, Dict, Mapping

from .utils import strip_values, PYPELINE_LOGGER


LOG = logging.getLogger(PYPELINE_LOGGER)

Column = namedtuple("Column", ("label", "in_key"))
LABEL_ACTIVITY_TYPE = "activityType"
LABEL_BOOTSTRAP_RULE = "bootstrapRule"
LABEL_WORKER_ID = "workerID"
LABEL_PARELLEL_PROCESSES = "parallelProcesses"
LABEL_FIRE_ON_FIRST_CYCLE = "onFirstCycle"
LABEL_ACTIVITY_DATA = "activityData"
BOOTSTRAP_CONFIG_FILE_NAME = "activity_bootstrap.csv"
BOOTSTRAP_CONFIG_HEADER = [
    Column(LABEL_ACTIVITY_TYPE, True),
    Column(LABEL_WORKER_ID, True),
    Column(LABEL_BOOTSTRAP_RULE, False),
    Column(LABEL_FIRE_ON_FIRST_CYCLE, False),
    Column(LABEL_ACTIVITY_DATA, True),
]
ACTIVITY_PROCESSING_CONFIG_FILE_NAME = "activity_processing.csv"
ACTIVITY_PROCESSING_CONFIG_HEADER = [
    Column(LABEL_ACTIVITY_TYPE, True),
    Column(LABEL_WORKER_ID, True),
    Column(LABEL_PARELLEL_PROCESSES, False),
]

ACTIVITY_BOOTSTRAP_META_HEADER = """#
# Activity bootstrap
# ==================
#
# Empty rows and anything written after a # character will be ignored
#
# This configuration file must contain an entry for each activity activity type,
# worker that may bootstrap process them, and optionally the data to put in the
# activity file.
#
# The macro $PYPELINE_DIR is available in the activityData field to inject
# the root directory of activities.
#
"""
ACTIVITY_PROCESSING_META_HEADER = """## Activity Processing
# ===================
#
# Empty rows and anything written after a # character will be ignored
#
# This configuration file must contain an entry for each activity activity type
# and worker that may want to process them, in order to specify which which
# workers may process activities and how much parallelism is allowed.
#
"""
DECODE_BOOL = lambda s: s.lower() == "true"


RuleEngineTypes = str | bool | int | float
RuleEngineValuePattern = re.compile(
    r"(\d+\.\d+)|(\d+)|(true)|(false)|(.*)", flags=re.IGNORECASE
)
RuleEngineValuePatternDecode: dict[int, Callable[[str], RuleEngineTypes]] = {
    1: float,
    2: int,
    3: DECODE_BOOL,
    4: DECODE_BOOL,
    5: lambda x: x,
}


class NoRuleMatchError(Exception):
    """When a RuleEngine getMapping call fails to match a rule"""


class RuleEngine:
    """Rule-table-based configuration engine"""

    configuration_file: Path
    """Path to file to read the configuration from"""
    columns_in_key: set[str]
    # header: list[str]
    # """Header of the configuration file"""

    UNIVERSAL_VALUE = "*"

    def __init__(
        self,
        configuration_file: Path,
        columns: list[Column],
        default_rows: list[Dict[str, str]] | None = None,
        meta_header: str | None = None,
    ) -> None:
        self.configuration_file = configuration_file
        self.columns_in_key = {c.label for c in columns if c.in_key}

        if configuration_file.exists():
            _header, _rows = self.read_file_contents()
            valid_header = len(_header) == len(columns) and all(
                k.label in _header for k in columns
            )
            if not valid_header:
                raise ValueError(
                    f"Found header in file {configuration_file} is invalid: {_header} != {[c.label for c in columns]}"
                )
            if default_rows:
                for expected_row in default_rows:
                    _criteria = {
                        k: v
                        for k, v in expected_row.items()
                        if k in self.columns_in_key
                    }
                    for _row in DictReader(_rows, fieldnames=_header):
                        if RuleEngine.row_matches_criterias(_row, _criteria):
                            break
                    else:
                        raise NoRuleMatchError(
                            f"Can't match {_criteria} in {self.configuration_file}"
                        )

        else:
            configuration_file.write_text(
                ("\n".join(meta_header) if meta_header else "")
                + ",".join(c.label for c in columns)
                + "\n"
                + (
                    "\n".join(
                        ",".join(r[c.label] for c in columns) for r in default_rows
                    )
                    if default_rows
                    else ""
                ),
                encoding="utf-8",
            )
            LOG.warning(
                "New configuration file %s created. Please end execution and fill it before re-running your program.",
                configuration_file,
            )

    @staticmethod
    def decode_cell(value: str) -> RuleEngineTypes:
        """Decodes cell value to a supported type"""
        _match = RuleEngineValuePattern.fullmatch(value)
        if _match is None or _match.lastindex is None:
            raise ValueError(f"Value '{value}' is invalid")
        decoder = RuleEngineValuePatternDecode[_match.lastindex]
        res = decoder(value)
        # LOG.info("Decoded '%s' as %s (%s)", value, res, type(res))
        return res

    @staticmethod
    def row_matches_criterias(
        row: dict[str, str], criteria: Mapping[str, RuleEngineTypes]
    ) -> bool:
        """Verifies whether the row matches criterias"""
        _matches = True
        for k, expected_value in criteria.items():
            _val = row[k]
            if _val == RuleEngine.UNIVERSAL_VALUE:
                continue
            _decoded_val = RuleEngine.decode_cell(_val)
            if _decoded_val == expected_value:
                continue
            LOG.debug(
                "Failed to match row %s with criteria %s on column %s: expected %s (%s) but found %s (%s)",
                row,
                criteria,
                k,
                expected_value,
                type(expected_value),
                _decoded_val,
                type(_decoded_val),
            )
            _matches = False
        return _matches

    def read_file_contents(self) -> tuple[list[str], list[str]]:
        """Reads CSV file with header"""
        rows = list(
            map(
                str.strip,
                self.configuration_file.read_text(encoding="utf-8").splitlines(),
            )
        )
        if len(rows) == 0:
            raise ValueError(f"Empty file {self.configuration_file}")
        # remove comments
        rows = list(
            filter(
                lambda r: len(r) > 0,
                map(
                    lambda r: (r[:idx] if (idx := r.find("#")) >= 0 else r).strip(),
                    rows,
                ),
            )
        )
        return strip_values(rows[0].split(",")), rows[1:] if len(rows) > 1 else []

    def get_mappings(
        self, criteria: dict[str, RuleEngineTypes], values: list[str]
    ) -> list[dict[str, RuleEngineTypes]]:
        """Selects the last rule that matches criteria and returns selected values"""
        _header, _rows = self.read_file_contents()
        _values = strip_values(values)

        # Sanity check
        unavailable_values = [v in _header for v in _values if v not in _header] + [
            c for c in criteria.keys() if c not in _header
        ]
        if unavailable_values:
            raise ValueError(
                f"Can't read values {unavailable_values} from table in file {self.configuration_file}"
            )

        res = [
            {k: RuleEngine.decode_cell(row[k]) for k in values}
            for row in DictReader(_rows, fieldnames=_header)
            if RuleEngine.row_matches_criterias(row, criteria)
        ]
        if not res:
            raise NoRuleMatchError(
                f"Can't match a rule in file {self.configuration_file} with criteria {criteria}"
            )

        return res

    def get_mapping(
        self, criteria: dict[str, RuleEngineTypes], values: list[str]
    ) -> dict[str, RuleEngineTypes]:
        """Selects the last rule that matches criteria and returns selected values"""
        return self.get_mappings(criteria, values)[-1]

    def get_single_mapping(
        self, criteria: dict[str, RuleEngineTypes], value: str
    ) -> RuleEngineTypes:
        """Shorthand for get_mapping when only one value needs to be fetched"""
        return self.get_mapping(criteria, [value])[value]
