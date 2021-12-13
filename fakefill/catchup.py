# standard library
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import NoReturn

# pypi/conda library
from sqlalchemy.exc import IntegrityError

try:
    # pypi/conda library
    from pendulum import Pendulum
except Exception:
    from pendulum import DateTime as Pendulum  # noqa

# pypi/conda library
from pytz import utc

# airflow library
from airflow.utils.state import State

# fakefill plugin
from fakefill.helpers.afutils import fetch_dag, gen_run_id, get_last_execution, trans_to_datetime
from fakefill.helpers.cfutils import Datetime, check_recent, parse_bool, parse_date, read_config
from fakefill.helpers.cronvert import cron_counts
from fakefill.helpers.logging import getLogger

logger = getLogger("catchup")


def fakefill(
    dag_id: str,
    start_date: Datetime,
    maximum_day: int,
    maximum_unit: int,
    config_path: str,
    i: bool,
    p: bool,
    y: bool,
    v: bool,
) -> NoReturn:
    # Set default
    ok_dag = 0
    dag_id = dag_id.lower().strip()

    # Read only not ""
    if config_path:
        configs = read_config(config_path)
    else:
        configs = {}

    # General settings
    start_date = parse_date(configs.get("settings", {}).get("start_date", start_date)) - timedelta(days=180)
    maximum_day = int(configs.get("settings", {}).get("maximum_day", maximum_day))
    maximum_unit = int(configs.get("settings", {}).get("maximum_unit", maximum_unit))
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
            ok_task = 0

            # Subdag will be ignored
            if dag.is_subdag:
                continue

            # if not fill all schedules flag and has latest execution date, start from the recent execution date
            if ignore and check_recent(get_last_execution(dag)):
                continue

            # If schedule is None: set external trigger to True
            if dag.schedule_interval:
                # get all the schdule starting from the given date
                run_dates = dag.get_run_dates(start_date)
                run_dates = [trans_to_datetime(rd) for rd in run_dates]
                run_dates = [rd for rd in run_dates if rd]
                run_dates.reverse()
                external_trigger = False

                num = (datetime.utcnow().replace(tzinfo=utc) - start_date).days
                num = min(num, maximum_day) if maximum_day else num

                # Maximum unit: set by crontab + maximum_xxx
                process_num, daily_unit = cron_counts(dag.schedule_interval)
                process_num = process_num * num if process_num <= 744 else daily_unit * num
                process_num = min(maximum_unit, process_num)

                if run_dates:
                    run_dates = run_dates[:process_num] if len(run_dates) > process_num else run_dates
                else:
                    run_dates = [Pendulum.utcnow().replace(hour=0, minute=0, second=0)]
            else:
                fake_last_execution = (datetime.utcnow() - timedelta(days=1)).replace(tzinfo=utc)
                run_dates = [fake_last_execution]
                external_trigger = True

            logger.info(f"{dag_id} has {len(run_dates)} tasks to be backfill")

            for date in run_dates:
                try:
                    sdate = execution_date = date
                    # generate run id -> migration_yyyy-mm-ddthh:mm:ss+00:00
                    run_id = gen_run_id(start_date)
                    dag.create_dagrun(
                        run_id=run_id,
                        state=State.SUCCESS,
                        execution_date=execution_date,
                        start_date=sdate,
                        external_trigger=external_trigger,
                    )
                except IntegrityError:
                    ok_task += 1
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
