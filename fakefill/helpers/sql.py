query_template = """insert into dag_run(dag_id, execution_date, run_id, state, external_trigger)
values (
  '%(dag_id)s',
  '%(execution_date)s',
  '%(run_id)s',
  'success',
  'false'
);"""
