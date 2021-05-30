"""
Automize backfill for Airflow

command examples:
    - backffill -d all -p -m 30 -y
    - backffill -d paco_bsf
explain:
    - run fakefill for the past 30 days without prompt, and only fill if all the dags which have status == pause
    - run fakefill for dag id == `paco_bsf` with maximum default backfill days == 365
note that the run id will be `migration_yyyy-mm-ddthh:mm:ss+00:00`
"""

# standard library
from pathlib import Path

# pypi/conda library
import click

# fakefill plugin
from fakefill.catchup import Datetime, fakefill
from fakefill.helpers.cfutils import parse_date_cli
from fakefill.helpers.logging import getLogger
from fakefill.helpers.template import gen_template

logger = getLogger("cfutils")


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "dag_id", "-d", default="", type=click.STRING, help="the dag name you want to backfill [dag_id or all]",
)
@click.option(
    "start_date",
    "-sd",
    default="",
    type=click.STRING,
    help="start date you want to backfill, default will fetch the start_date defined in the config of that dag",
    callback=parse_date_cli,
)
@click.option(
    "maximum_day", "-md", default=180, type=click.IntRange(min=0, max=180, clamp=True), help="maximum days to backfill",
)
@click.option(
    "maximum_unit",
    "-mu",
    default=60 * 24 * 30,
    type=click.IntRange(min=1, max=60 * 24 * 30, clamp=True),
    help="max unit (based on the crontab) to backfill",
)
@click.option("config_path", "-cp", default="", type=click.STRING, help="config for auto fastfill if have one")
@click.option("-i", default=False, is_flag=True, help="fill all ignore it just ran recently")
@click.option("-p", default=False, is_flag=True, help="only fill paused dags")
@click.option("-y", default=False, is_flag=True, help="confirm by default")
@click.option("-v", default=False, is_flag=True, help="print traceback if got error")
def run(
    dag_id: str,
    start_date: Datetime,
    maximum_day: int,
    maximum_unit: int,
    config_path: str,
    i: bool,
    p: bool,
    y: bool,
    v: bool,
):
    ctx = click.get_current_context()

    if not dag_id and not Path(config_path).is_file():
        logger.error("Need to assign a dag id or a path to config yaml")
        ctx.abort()

    fakefill(dag_id, start_date, maximum_day, maximum_unit, config_path, i, p, y, v)


@cli.command()
@click.option("template_path", "-p", default="", type=click.STRING, help="Generate a config template yaml")
def template(template_path):
    gen_template(path=template_path)


if __name__ == "__main__":
    cli()
