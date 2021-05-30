# standard library
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, TypeVar, Union

# pypi/conda library
from pytz import utc
from yaml import unsafe_load

# fakefill plugin
from fakefill.helpers.logging import getLogger

logger = getLogger("cfutils")


Datetime = TypeVar("datetime", bound=datetime)
DefaultDate: Datetime = datetime.strptime(
    f"{(datetime.utcnow().date() - timedelta(days=365))} 00:00:00", "%Y-%m-%d %H:%M:%S"
).replace(tzinfo=utc)


def parse_date_cli(ctx, param, conf) -> Datetime:
    if conf:
        return parse_date(conf)
    return ""


def parse_format(time_str):
    datetime_group = re.split(r"(<?\s|T)", time_str, 1)

    date_units = ["%Y", "%m", "%d"]
    time_units = ["%H", "%S", "%M"]

    # Date Part
    # split by possible + normal sep
    date = datetime_group[0]
    date_group = re.split(r"(<?\s|-|/)", date)

    # clean up
    sep = "-" if "-" in date_group else "/" if "/" in date_group else " " if " " in date_group else "-"
    date = [d for d in date_group if d.isdigit()]

    # rejoin
    date_group = sep.join(date)
    date_fmt = sep.join(date_units[: len(date)])

    if len(datetime_group) == 1:
        return date_group, date_fmt

    # Time Part
    # split by possible + normal sep
    time = datetime_group[2]
    time_group = re.split(r"(<?\s|:|-)", time)

    sep = ":" if ":" in time_group else "-" if "-" in time_group else " " if " " in time_group else ":"
    time = [t for t in time_group if t.isdigit()]

    time_group = sep.join(time)
    time_fmt = sep.join(time_units[: len(time)])

    return f"{date_group} {time_group}".strip(), f"{date_fmt} {time_fmt}".strip()


def parse_date(conf) -> Datetime:
    try:
        if isinstance(conf, datetime):
            date = conf.replace(tzinfo=utc)
        elif isinstance(conf, str) and conf:
            datetime_group, fmt_group = parse_format(conf)
            date = datetime.strptime(datetime_group, fmt_group).replace(tzinfo=utc)
        else:
            date = DefaultDate
    except Exception:
        logger.warning(f"Unable to parse the given time format, return default value: {DefaultDate}")
        date = DefaultDate
    else:
        return date


def parse_bool(conf: Union[bool, str]) -> bool:
    if isinstance(conf, bool):
        return conf
    elif conf.lower().strip() == "true":
        return True
    return False


def check_recent(lastrun: datetime) -> bool:
    if lastrun:
        now = datetime.utcnow().replace(tzinfo=utc)
        if (now - lastrun).days < 3:
            return True
    return False


def read_config(config_path) -> Union[Dict, None]:
    if not Path(config_path).is_file:
        raise FileNotFoundError

    with open(f"{Path(config_path)}") as file:
        raw_yaml = file.read()

    configs = unsafe_load(raw_yaml)

    return configs
