# standard library
from datetime import datetime
from pathlib import Path
from typing import Dict, TypeVar, Union

# pypi/conda library
from pytz import utc
from yaml import unsafe_load

# afill plugin
from afill.helpers.logging import getLogger

logger = getLogger("cfutils")


Datetime = TypeVar("datetime", bound=datetime)
DefaultDate: Datetime = datetime.strptime("2020-09-01 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)


def parse_date_cli(ctx, param, conf) -> Datetime:
    return parse_date(conf)


def parse_date(conf) -> Datetime:
    try:
        if isinstance(conf, datetime):
            date = conf.replace(tzinfo=utc)
        elif isinstance(conf, str) and conf:
            date = datetime.strptime(conf, "%Y-%m-%d").replace(tzinfo=utc)
        else:
            date = DefaultDate
    except Exception:
        logger.warning("Date need to follow pattern: %Y-%m-%d")
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
