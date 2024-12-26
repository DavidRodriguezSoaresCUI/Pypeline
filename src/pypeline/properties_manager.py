import logging
import re
from pathlib import Path

from .utils import FileDefinedValue, Singleton, PYPELINE_LOGGER

LOG = logging.getLogger(PYPELINE_LOGGER)


class PropertiesManager(metaclass=Singleton):
    """Used to read properties file

    A property file may contain:
    - empty lines
    - comments (anything after a # symbol)
    - key-value pairs (separated by equals sign; there can be whitespace sourrounding the equals sign)
    Multi-line values and other non-empty non-key-value pair lines will cause an exception to be raised.
    Note: if a key part is `*` (wildcard), it matches any value; if multiple keys match, the one with the
    lesser amount of wildcards will be selected
    """

    property_files: list[FileDefinedValue[str]]
    """List of property files"""
    properties: dict[tuple[str], str]
    """Properties read from files"""

    PROPERTY_DEFINITION = re.compile(r"^([^=]+)=([^=]+)$")
    WILDCARD_PROPERTY_PART = "*"
    PROPERTY_PREFIX_ATTRIBUTE = "__property_prefix__"

    def __init__(self, property_files: list[Path]) -> None:
        self.property_files = [
            FileDefinedValue[str](_file, lambda f: f.read_text(encoding="utf-8"))
            for _file in property_files
        ]
        self.properties = {}
        self.reload()

    def reload(self) -> None:
        """Parses all properties files"""
        if any(p.should_reload_data() for p in self.property_files):
            self.properties = {}
            for _file in self.property_files:
                self.properties.update(self.read_properties_file(_file))
            LOG.debug(
                "Loaded properties: files=%s properties=%s",
                self.property_files,
                self.properties,
            )

    @classmethod
    def resolve_property_name(
        cls, obj, partial_or_full_property_name: str | list[str]
    ) -> list[str]:
        """Resolves property name as a list of components"""
        res = (
            partial_or_full_property_name
            if isinstance(partial_or_full_property_name, list)
            else partial_or_full_property_name.split(".")
        )
        if hasattr(obj, cls.PROPERTY_PREFIX_ATTRIBUTE):
            prefix = getattr(obj, cls.PROPERTY_PREFIX_ATTRIBUTE)
            res = [
                part.strip()
                for part in (
                    prefix.split(".")
                    if prefix is not None and isinstance(prefix, str)
                    else []
                )
            ] + res
        return res

    @staticmethod
    def read_properties_file(
        property_file: FileDefinedValue[str],
    ) -> dict[tuple[str], str]:
        """Reads a property file and returns its parsed contents"""
        res: dict[tuple[str], str] = {}
        for line_idx, line in enumerate(property_file.get().splitlines()):
            _line = line

            # Remove comments
            if (comm_idx := _line.find("#")) >= 0:
                _line = _line[:comm_idx]

            # Ignore empty lines
            _line = _line.strip()
            if len(_line) == 0:
                continue

            _match = PropertiesManager.PROPERTY_DEFINITION.match(_line)
            if _match:
                key: tuple[str] = tuple(x.strip() for x in _match.group(1).split("."))  # type: ignore[assignment]
                res[key] = _match.group(2).strip()
            else:
                raise ValueError(
                    f"File '{property_file.source_file}' line {line_idx}: failed to parse content ('{_line}')"
                )

        return res

    def resolve_property(self, property_name: list[str], required: bool) -> str | None:
        """Checks if property exists and returns it if so. Raises ValueError on missing property"""

        self.reload()

        candidate_props = list(
            p for p in self.properties if len(p) == len(property_name)
        )
        for part_idx, part in enumerate(property_name):
            candidate_props = [
                c
                for c in candidate_props
                if c[part_idx] in (self.WILDCARD_PROPERTY_PART, part)
            ]
            if len(candidate_props) == 0:
                if required:
                    raise ValueError(
                        f"Property {property_name} doesn't exist in files {self.property_files}"
                    )
                return None

        selected_candidate = min(
            candidate_props,
            key=lambda c: sum(1 for _part in c if _part == self.WILDCARD_PROPERTY_PART),
        )

        return self.properties[selected_candidate]

    def get_string(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        required: bool = False,
        default: str | None = None,
    ) -> str | None:
        """Gets property; if not `required`, then may return None, otherwise either returns str else raises ValueError"""
        res = self.resolve_property(
            self.resolve_property_name(obj, partial_or_full_property_name),
            required,
        )
        if res is None:
            return default
        return res

    def get_bool(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        default: bool,
    ) -> bool:
        """Gets property as a boolean"""
        prop_name = self.resolve_property_name(obj, partial_or_full_property_name)
        prop = self.resolve_property(
            prop_name,
            required=False,
        )
        return default if prop is None or len(prop) == 0 else prop.lower() == "true"

    def get_int(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        default: int,
    ) -> int:
        """Gets property as an integer"""
        prop_name = self.resolve_property_name(obj, partial_or_full_property_name)
        prop = self.resolve_property(
            prop_name,
            required=False,
        )
        try:
            return int(prop)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return default

    @property
    def source_files(self) -> list[Path]:
        """Returns a list of property files"""
        return [fdv.source_file for fdv in self.property_files]
