# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

<!-- As much as possible use subsections: Added, Removed, Modified, BugFix -->

## [0.1.0] - 2025-08-17

> Note : Again this project has been developed for some time without being pushed so there are a lot of changes

- change artifact from `pypeline-DavidRodriguezSoaresCUI` to `drs.pypeline`
- complete `README.md` description
- rebuild using updated `python-project-quickstart`
- added minimal example in `example` folder
- reorganized classes into files
- activity retry : added optional delay, implemented as optional datetime field in activity name
- Added abstract base class `ActivityData` to provide serialization-related facilities for activity data classes. With usage example. Combined with `@dataclass` annotation, allows for implementation as simple as field declaration.
- Added `ActivityCreator` singleton which handles all activity creation tasks, so to take that responsibility away from the `Orchestrator` class.
- Focus on defining the activity type `NotificationActivity` to handle notifications from `Orchestrator` and other processors.
- Processor configuration : support for YAML configuration, added `PropertySpec` class to formalize property definition and documentation inside the processor and allow for automatic generation of configuration (YAML or properties) with defaults
- `PropertiesManager` : added reload cooldown property, `get_float` method ans methods to edit properties files
- `Orchestrator`: documentation of properties (doc and `PropertySpec`), automatic generation of `default.properties` file
- `TimedExecutionRule`: more accurate (but complex) "cronlite" validation pattern
- Added return codes to allow for external control such as automatic relaunch
- Added/updated documentation for some methods that needed it

## [0.0.1] - 2024-12-26

__INITIAL RELEASE__

> Note: This project has been developed for some time, including running for months on a test system, but was not versioned at the time and has been partially rewritten since. At this stage I have taken the decision to publish it, but it is far from ready for production and is still in development and core aspects can change without notice.