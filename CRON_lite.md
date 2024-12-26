# CRONlite

For simplicity (of usage and implementation) reasons, Pypeline's activity bootstrap focuses on a time resolution range that goes from the minute to the day of the week. For that purpose I propose the following syntax for periodical task scheduling:

```
┌─── minute
| ┌─── hour
| | ┌─── day of the week
| | |
| | |
* * *
```

## Specifications
Field specifications :

Field | Allowed values | Allowed special characters
---|---|---
minute | ``0-59`` | ``*`` ``,``
hour | ``0-23`` | ``*`` ``,``
day of the week | ``0-6`` or ``SUN``-``SAT`` | ``*`` ``,``

Special character specifications :

Special character | Description | Example
---|---|---
**Asterisk/wildcard (``*``)** | Represents "all" or "every" | ``0 * MON`` will run every hour (at 0 minutes) on mondays
**Comma (``,``)** | Allows to give a list of values instead of just one | ``0 6,15,22 *`` will run at 6,15 and 22 hours (at 0 minutes) every day

## Macros

For ease of use and readability, the macro ``@every`` can be used for scheduled tasks whose time constraints is limited to their frequency. Syntax:

```
       ┌─── value
       |┌─── time resolution
       ||
       ||
@every **
```

With specifications:

Time resolution | Allowed values | Example
---|---|---
``m`` (minute) | ``0-59`` | ``@every 15m`` will run every 15 minutes, with first execution at startup
``h`` (hour) | ``0-23`` | ``@every 2h`` will run every 2 hours, with first execution at startup