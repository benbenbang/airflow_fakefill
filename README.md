# Airflow Fastfill Marker



Due to migrating to Kubernetes-host Airflow and using different backend, we need to find out a way to fill out all the history since its starting date for thousands of dags. To make this process going faster and easier, in the meantime, I didn't find this kind of tool on Github, so I implement this simple tool to help with marking dags as `success.` Hope it can also help others.



## Installations

### Method 1

```bash
$ pip install fakefill
```

### Method 2

```bash
$ pip install git+https://git@github.com/benbenbang/airflow_fastfill.git
```

### Method 3

```bash
$ git clone git@github.com:benbenbang/airflow_fastfill.git
$ cd airflow_fastfill
$ pip install .
```



## Usages

```bash
$ fakefill
```

It takes 1 of 2 required argument, and 6 optional arguments. You can also define them in a yaml file and pass to the cli.

- Options

    - Required [1 / 2]:

        > - dag_id [-d][reqired]: can be a real dag id or "all" to fill all the dags
        > - config_path [-cp][choose one]: path to the config yaml

    - Optional:
        >- start_date [-sd]: starting date, default will be counted from 365 days ago
        >- maximum_day [-md]: maximum fill date per dag, rangint: [1, 180]
        >- maximum_unit [-mu]: maxium fill unit per dag, rangint: [1, 43200]
        >- ignore [-i]: still procceed auto fill even the dag ran recently
        >- pause_only [-p]: pass true to fill dags which are pause
        >- confirm [-y]: pass true to bypass the prompt if dag_id is all
        >- traceback [-v]: pass print our Airflow Database error



## Examples

Fill all the dags for the past 30 days without prompt, and only fill if all the dags which have status == pause

```bash
$ fakefill -d all -p -md 30 -y
```



Run fastfill for dag id == `dag_a` by counting default fakefill days == 365

```bash
$ fakefill -d dag_a
```



Run fastfill with config yaml

```bash
$ fakefill -cp config.yml
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
