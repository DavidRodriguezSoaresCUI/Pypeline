# [pypeline](https://github.com/DavidRodriguezSoaresCUI/pypeline) - A simple infrastructure for asynchronous task planning and execution

`Pypeline` was originally developed to run arbitrary tasks on locked-down linux-based systems with very limited resources besides a `Python` interpreter.

Specific notions and terminology:

- `activities` : Represent a task to be accomplished in the form of a JSON file, which holds the data describing specifics of the task instance.
- `processor` : Contain the code to process activities of a given type.

> Each activity has a discrete type, and each "activity type" requires a dedicated `processor`

Main features :
- Automate any action : downloading files, running commands, etc. If you can write it in ``Python``, you can run it with ``Pypeline``
- Division of responsibilities : Trust `Pypeline` for the scheduling, work distribution, activity tracking and other shenanigans, so you can focus on the fun part (*code that performs work*).
- Chain actions : `processors` have the ability to create arbitrary new `activities` (eg: download a media file, then copy/rename it to the user library, then update the library index, then perform a backup, then send a message to the user)
- Configurability : `processors` have access to facilities to fetch user-defined configuration variables to alter their behavior (see `Usage > Properties Manager` section below)
- Distributed computing : Multiple instances can operate on a single activity repository, for example to run specific workloads on specific machines (ex: transcoding on a machine that has hardware acceleration available, running memory-intensive computation on a machine with the required amount of RAM)
- Managing `activities` : it is possible to assign a `processor` dedicated to handling failed activities (ex: retrying or notifying the user)

For basic tasks like running a command, `Pypeline` offers built-in processors (see <a href="src/pypeline/processors.py">src/pypeline/processors.py</a>)

## Requirements

- A ``Python`` interpreter/runtime. This project was developed for Python 3.10 and may not work on lower versions.
- A filesystem with available space for `activity` files

## Installation

From a terminal execute:

```bash
python -m pip install drs.pypeline
```

> Note that on some systems it may be necessary to specify python version as `python3`

### Installation from source

Download/``git clone`` the project, open it in a terminal end execute ``python -m pip install .``

## Run tests

First some requirements (step 3 needs to be re-run if you make changes) :

1. Download/``git clone`` the project and open it in a terminal
2. Install the testing requirements with ``python -m pip install .[test]``
3. Install the project with ``python -m pip install -e .``
   > It needs to be installed in editable mode for coverage to work

Then you can simply run ``pytest`` (from a terminal at the downloaded project directory)

## Usage

### Basic setup

A Python project using `pypeline` requires two components :

- a `data` directory, containing configuration files and files related to the activities
- the project itself

> An example project is available under `example`, and recommended activity data/processor architecture in documentation (build it and see ActivityData or see the <a href="src/pypeline/activity.py?plain=1#L438">docstring in code</a>)


