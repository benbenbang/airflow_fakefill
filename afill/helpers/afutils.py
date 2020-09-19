# standard library
import os
import sys
from typing import List, Tuple

# pypi/conda library
import click

# airflow library
from airflow.models import DAG, DagBag, DagModel
from airflow.utils.db import provide_session

from .cfutils import logger
from .exceptions import DagNotFoundError


def get_dag(dag) -> Tuple[str, DAG]:
    dag = dag.get_dag()
    return (dag.dag_id, dag)


def get_all_dags(get_pause_only: bool):
    airflow_home = os.environ["AIRFLOW_HOME"]
    dags_home = os.path.join(airflow_home, "dags", "dags")
    dagbag = DagBag(dags_home)
    if get_pause_only:
        dagbag = [(dag_id, dag) for dag_id, dag in dagbag.dags.items() if dag.is_paused]
    else:
        dagbag = [(dag_id, dag) for dag_id, dag in dagbag.dags.items()]
    return dagbag


def gen_run_id(start_date):
    date = start_date.strftime("%Y-%m-%d")
    time = start_date.strftime("%H:%M:%S")
    tz_info = start_date.strftime("%z")
    tz = f"{tz_info[:3]}:{tz_info[3:]}"
    run_id = f"migration__{date}T{time}{tz}"
    return run_id


@provide_session
def fetch_dag(session, dag_id: str, get_pause_only: bool, confirm: bool) -> List[Tuple[str, DAG]]:
    dags = []

    msg = "You are going to backfill all the dags" if dag_id == "all" else f"You are going to backfill {dag_id}?"

    if not confirm and dag_id:
        click.confirm(f"{msg}, sure about that?", abort=True)
    elif dag_id == "all":
        logger.warning(msg)

    try:
        if dag_id and dag_id != "all":
            dags = session.query(DagModel).filter(DagModel.dag_id == dag_id).all()
            if dags:
                dags = [get_dag(dag) for dag in dags]
            else:
                raise DagNotFoundError
        elif dag_id == "all":
            dags = get_all_dags(get_pause_only)
        else:
            logger.error(f"Unable to fetch dag(s). Need to assign a dag id")
            sys.exit(-1)
    except Exception as e:
        raise e
    else:
        return dags
