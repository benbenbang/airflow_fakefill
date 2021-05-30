# standard library
from pathlib import Path

# pypi/conda library
import yaml

# fakefill plugin
from fakefill.helpers.logging import getLogger

logger = getLogger("template")

tmpl = {
    "dags": ["dag_a", "dag_b"],
    "settings": {"start_date": "2019-01-01", "maximum": 365, "traceback": False, "confirm": True, "pause_only": True},
}


def gen_template(path: str):
    try:
        path = path or Path().cwd() / "config.yml"
        fp = Path(path)

        with open(fp, "w") as file:
            yaml.dump(tmpl, file, indent=2, sort_keys=False)

    except Exception:
        logger.error("Unable to generate template")

    else:
        logger.success(f"Template has been generated at {fp}")
