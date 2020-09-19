# Airflow Fastfill Marker



Due to migrating to Kubernetes-host Airflow and using different backend, we need to find out a way to fill out all the history since its starting date for thousands of dags. To make this process going faster and easier, in the meantime, I didn't find this kind of tool on Github, so I implement this simple tool to help with marking dags as `success.` Hope it can also help others.



## Usages

```bash
$ afill
```

It takes 1 of 2 required argument, and 6 optional arguments. You can also define them in a yaml file and pass to the cli.

- Options

    - Required [1 / 2]:

        > - dag_id [-d]: can be a real dag id or "all" to fill all the dags
        > - config_path [-cp]: path to the config yaml

    - Optional:

        >- start_date [-sd]: starting date, will use the one from default args if not given
        >- maximum [-m]: maximum fill number per dag
        >- ignore [-i]: still procceed auto fill even the dag ran recently
        >- pause_only [-p]: pass true to fill dags which are pause
        >- confirm [-y]: pass true to bypass the prompt if dag_id is all
        >- traceback [-v]: pass print our Airflow Database error



## Examples

Fill all the dags for the past 30 days without prompt, and only fill if all the dags which have status == pause

```bash
$ afill -d all -p -m 30 -y
```



Run fastfill for dag id == `dag_a` with maximum default backfill days == 365

```bash
$ afill -d dag_a
```



Run fastfill with config yaml

```bash
$ afill -cp config.yml
```

The yaml file needs to be defined with two dictonary types: `dags` and `settings`. For `dags` section, it needs to be a `list`, while the `settings`section is `dict`

Sample:

```yaml
dags:
  - dag_a
  - dag_b
  - dag_c

settings:
  start_date: 2019-01-01
  maximum: "365"
  traceback: false
  confirm: true
  pause_only: true

```
