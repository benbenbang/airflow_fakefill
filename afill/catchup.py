# standard library
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import NoReturn

# pypi/conda library
from pendulum import Pendulum
from pytz import utc

# airflow library
from airflow.utils.state import State

# afill plugin
from afill.helpers.afutils import fetch_dag, gen_run_id
from afill.helpers.cfutils import Datetime, check_recent, parse_bool, parse_date, read_config
from afill.helpers.cronvert import cron_counts
from afill.helpers.logging import getLogger

logger = getLogger("catchup")


def fastfill(
    dag_id: str, start_date: Datetime, maximum: int, config_path: str, i: bool, p: bool, y: bool, v: bool
) -> NoReturn:
    # Set default
    ok_dag = ok_task = 0
    dag_id = dag_id.lower().strip()

    # Read only not ""
    if config_path:
        configs = read_config(config_path)
    else:
        configs = {}

    # General settings
    start_date = parse_date(configs.get("settings", {}).get("start_date", start_date))
    maximum = int(configs.get("settings", {}).get("maximum", maximum))
    ignore = parse_bool(configs.get("settings", {}).get("ignore", i))
    pause_only = parse_bool(configs.get("settings", {}).get("pause_only", p))
    confirm = parse_bool(configs.get("settings", {}).get("comfirm", y))
    traceback = parse_bool(configs.get("settings", {}).get("traceback", v))

    # Dags settings
    dags_yml = configs.get("dags", [])
    run_only = dags_yml.get("run_only", []) if isinstance(dags_yml, dict) else None
    exclude_dags = dags_yml.get("excludes", []) if isinstance(dags_yml, dict) else []

    # fetch dag
    if run_only:
        dagbag = [fetch_dag(dag_id=dag_id, get_pause_only=pause_only, confirm=confirm) for dag_id in run_only]
    elif dag_id:
        dagbag = fetch_dag(dag_id=dag_id, get_pause_only=pause_only, confirm=confirm)
    else:
        raise ValueError(
            "Cannot find any dag_id. Make sure you passed the right config file or try `-d` to pass dag_id"
        )

    dagbag = [dag for dag in dagbag if dag[0] not in exclude_dags]

    if len(dagbag) == 0:
        logger.warning("Unable to fetch any dag by the given dag id(s)")
        sys.exit(-1)
    else:
        logger.info(f"Got {len(dagbag)} dags to process")

    for dag_id, dag in dagbag:
        try:
            # if not fill all schedules flag and has latest execution date, start from the recent execution date
            if check_recent(dag.latest_execution_date) and not ignore:
                start_date = dag.latest_execution_date
                delta = 0
            else:
                # get default date from the config.yml, if not backfill starting from 2020-09-01
                start_date = datetime.fromtimestamp(dag.default_args.get("start_date", start_date).timestamp())
                delta = (datetime.utcnow() - start_date).days > maximum
                # backfill no more than n days
                if delta:
                    start_date = datetime.utcnow() - timedelta(days=maximum)

            # If schedule is None: set external trigger to True
            if dag.schedule_interval:
                # get all the schdule starting from the given date
                run_dates = dag.get_run_dates(start_date)
                run_dates = [rd for rd in run_dates if not isinstance(rd, Pendulum)]
                run_dates.reverse()
                external_trigger = False
                if delta:
                    # Maximum unit: set by crontab
                    maximum = cron_counts(dag.schedule_interval, delta)
                    run_dates = run_dates[:maximum]
            else:
                fake_last_execution = (datetime.utcnow() - timedelta(days=1)).replace(tzinfo=utc)
                run_dates = [fake_last_execution]
                external_trigger = True

            logger.info(f"{dag_id} has {len(run_dates)} tasks to be backfill")

            for date in run_dates:
                try:
                    start_date = execution_date = date
                    # generate run id -> migration_yyyy-mm-ddthh:mm:ss+00:00
                    run_id = gen_run_id(start_date)
                    dag.create_dagrun(
                        run_id=run_id,
                        state=State.SUCCESS,
                        execution_date=execution_date,
                        start_date=start_date,
                        external_trigger=external_trigger,
                    )
                except Exception:
                    logger.debug(f"cannot auto backfill for {dag_id} on date {execution_date}")
                    pass
                else:
                    ok_task += 1
                    sleep(0.5)

        except Exception:
            message = f"Cannot backfill dag: {dag_id}"
            if traceback:
                logger.exception(message)
            else:
                logger.error(message)
        else:
            ok_dag += 1
            sleep(5)
        finally:
            logger.info(f"Total processed: {ok_task}")
    else:
        if ok_dag == len(dagbag):
            msg = "Succeed to auto backfill all the dags" if ok_dag > 1 else "Succeed to auto backfill dag: {dag_id}"
            logger.success(msg)
        else:
            logger.warning(f"Succeed to process {ok_dag} dags, and {len(dagbag) - ok_dag} failed")
