# [pypeline](https://github.com/DavidRodriguezSoaresCUI/pypeline) - A simple infrastructure for asynchronous task planning and execution

Put a description here

## Requirements

This project was developed for Python 3.10 and may not work on lower versions.

## Installation

From a terminal execute:

```bash
python -m pip install pypeline-DavidRodriguezSoaresCUI
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