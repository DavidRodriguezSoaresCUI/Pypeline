from collections import namedtuple
from dataclasses import dataclass
from io import StringIO
import logging
import re
from pathlib import Path
from time import time
from typing import Any

from .utils import (
    FileDefinedValue,
    Singleton,
    PYPELINE_LOGGER,
    dump_to_yaml,
    make_banner,
)

LOG = logging.getLogger(PYPELINE_LOGGER)


@dataclass
class PropertySpec:
    """Used to declare that a Processor may use a property by calling PropertiesManager"""

    parts: str | list[str]
    type: type | str
    help: str
    default: Any = None


def configuration_to_yaml(
    configuration_spec: list[PropertySpec], parent: str | None = None
) -> str:
    """Used for writing processor default configuration to yaml config file"""
    res = {}
    for cs in configuration_spec:
        name = "-".join(cs.parts) if isinstance(cs.parts, list) else cs.parts
        _default = (
            ""
            if cs.default is None or not isinstance(cs.default, (int, float, str, bool))
            else f"; default={cs.default}"
        )
        res[name + "-help"] = f"[{cs.type}{_default}] {cs.help}".strip()
        res[name] = cs.default

    if parent:
        res = {parent: res}

    return dump_to_yaml(res)


Property = namedtuple(
    "Property", field_names=["value", "property_file_idx", "line_idx"]
)


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
    properties: dict[tuple[str], Property]
    """Properties read from files"""
    last_reload_timestamp: float
    """To avoid reloading property files' properties too often"""

    PROPERTY_DEFINITION = re.compile(r"^([^=]+)=([^=]+)$")
    WILDCARD_PROPERTY_PART = "*"
    PROPERTY_PREFIX_ATTRIBUTE = "__property_prefix__"
    __property_prefix__: str = "PropertiesManager"
    RELOAD_COOLDOWN_MS_PROPERTY = ["reload-cooldown-ms"]
    RELOAD_COOLDOWN_MS_DEFAULT = 5000

    def __init__(
        self,
        property_files: list[Path],
    ) -> None:
        # Some validation
        if len(property_files) == 0:
            raise ValueError(
                "Can't initialize PropertiesManager without property files !"
            )
        incorrect_ext = [pf for pf in property_files if pf.suffix != ".properties"]
        if incorrect_ext:
            raise ValueError(
                f"Files have incorrect extension: expected='.properties' files={incorrect_ext}"
            )

        self.property_files = [
            FileDefinedValue[str](_file, lambda f: f.read_text(encoding="utf-8"))
            for _file in property_files
        ]
        self.properties = {}
        self.last_reload_timestamp = 0.0
        self.reload()

    def reload(self) -> None:
        """Parses all properties files"""

        # Skip if reload is in cooldown
        reload_cooldown_s = (
            self.get_int(
                self,
                self.RELOAD_COOLDOWN_MS_PROPERTY,
                self.RELOAD_COOLDOWN_MS_DEFAULT,
                no_reload=True,
            )
            / 1_000
        )
        if self.last_reload_timestamp + reload_cooldown_s > time():
            return

        if any(p.should_reload_data() for p in self.property_files):
            self.properties = {}
            for file_idx in range(len(self.property_files)):
                self.properties.update(self.read_properties_file(file_idx))
            LOG.debug(
                "Loaded properties: files=%s properties=%s",
                self.property_files,
                self.properties,
            )
            self.last_reload_timestamp = time()

    def insert_processor_sections_if_not_exist(
        self, processor, declared_properties: list[PropertySpec]
    ) -> None:
        """Simplifies properties file setup by inserting properties Processors declare into
        the main property file"""
        main_property_file = self.property_files[0]
        main_property_file_contents = main_property_file.get()
        previous_last_read_timestamp = main_property_file.last_read

        missing_props = {
            name: p
            for p in declared_properties
            if (name := self.resolve_property_name(processor, p.parts))
            not in main_property_file_contents
            not in self.properties
        }
        if missing_props:
            proc_name = processor.__name__
            LOG.info(
                "Will add missing processor properties: file=%s processor=%s properties=%s",
                main_property_file.source_file,
                proc_name,
                list(missing_props),
            )

            # Craft new lines to add
            proc_banner = make_banner(proc_name) + "\n"
            new_lines = []
            new_lines.append("\n\n")
            new_lines.append(proc_banner)
            for name, ppty in missing_props.items():
                _default = "" if ppty.default is None else f"; default={ppty.default}"
                new_lines.append(f"# [{ppty.type}{_default}] {ppty.help}\n")
                new_lines.append(f"#{name}=\n")
            new_lines_s = "".join(new_lines)

            # Reload main property file contents if necessary
            if previous_last_read_timestamp != main_property_file.last_read:
                main_property_file_contents = main_property_file.get()

            before_banner, *after_banner = main_property_file_contents.split(
                proc_banner, maxsplit=1
            )
            main_property_file.source_file.write_text(
                "".join([before_banner, new_lines_s, *after_banner]), encoding="utf8"
            )

    @classmethod
    def __resolve_property_name_parts(
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

    @classmethod
    def resolve_property_name(
        cls, obj, partial_or_full_property_name: str | list[str]
    ) -> str:
        """For external use; Returns the resulved full name of a property as a string"""
        return ".".join(
            PropertiesManager.__resolve_property_name_parts(
                obj, partial_or_full_property_name
            )
        )

    def read_properties_file(
        self,
        property_file_idx: int,
    ) -> dict[tuple[str], str]:
        """Reads a property file and returns its parsed contents"""
        res: dict[tuple[str], str] = {}
        property_file = self.property_files[property_file_idx]
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
                res[key] = Property(
                    value=_match.group(2).strip(),
                    property_file_idx=property_file_idx,
                    line_idx=line_idx,
                )
            else:
                raise ValueError(
                    f"File '{property_file.source_file}' line {line_idx}: failed to parse content ('{_line}')"
                )

        return res

    def __resolve_property(
        self, property_name: list[str], required: bool, no_reload: bool = False
    ) -> Property | None:
        """Checks if property exists and returns it if so. Raises ValueError on missing property"""

        if not no_reload:
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
        no_reload: bool = False,
    ) -> str | None:
        """Gets property; if not `required`, then may return None, otherwise either returns str else raises ValueError"""
        prop_name = self.__resolve_property_name_parts(
            obj, partial_or_full_property_name
        )
        ppty = self.__resolve_property(prop_name, required, no_reload=no_reload)
        if ppty is None:
            return default
        return ppty.value

    def get_bool(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        default: bool,
        no_reload: bool = False,
    ) -> bool:
        """Gets property as a boolean"""
        prop = self.get_string(
            obj, partial_or_full_property_name, required=False, no_reload=no_reload
        )
        return default if prop is None or len(prop) == 0 else prop.lower() == "true"

    def get_float(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        default: int,
        no_reload: bool = False,
    ) -> float:
        """Gets property as an integer"""
        prop = self.get_string(
            obj, partial_or_full_property_name, required=False, no_reload=no_reload
        )
        try:
            return float(prop)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return default

    def get_int(
        self,
        obj: object,
        partial_or_full_property_name: str | list[str],
        default: int,
        no_reload: bool = False,
    ) -> int:
        """Gets property as an integer"""
        prop = self.get_string(
            obj, partial_or_full_property_name, required=False, no_reload=no_reload
        )
        try:
            return int(prop)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return default

    def comment_property(
        self, obj: object, partial_or_full_property_name: str | list[str]
    ):
        """Comment property by editing a property file"""
        prop_name = self.__resolve_property_name_parts(
            obj, partial_or_full_property_name
        )
        ppty = self.__resolve_property(prop_name, required=False)
        if ppty:
            file_to_edit = self.property_files[ppty.property_file_idx]
            LOG.info(
                "Commenting line %s of file %s",
                ppty.line_idx + 1,
                file_to_edit.source_file,
            )
            file_to_edit.edit_content(
                lambda content: "\n".join(
                    "#" + line if line_idx == ppty.line_idx else line
                    for line_idx, line in enumerate(content.splitlines())
                )
            )

    @property
    def source_files(self) -> list[Path]:
        """Returns a list of property files"""
        return [fdv.source_file for fdv in self.property_files]
