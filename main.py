from utils import APIHelper
import os
from datetime import datetime
import time
import json
import pandas as pd
import logging
from train import get_score_model, train_model

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from contants import CLEAN_DATA_DIRECTORY, RAW_FILES_DIRECTORY, NUMBER_OF_WEATHER_REQUEST

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(processName)s][%(name)s]:%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)


def init():
    try:
        Variable.get("CITIES")
        logging.info(
            "Variable CITIES already defined. You can update it using Web interface 'Admin > Variables'")

    except:
        cities = "paris,london,washington"
        logging.info(
            f"Create variable with values: {cities}. You can update it using Web interface 'Admin > Variables'")
        Variable.set("CITIES", cities)


def get_weather():
    """
    Gets weather of the declated CITIES
    """

    logging.info("Getting API KEY.")
    api_key = os.getenv('OPEN_WEATHER_MAP_API_KEY')
    if api_key is None:
        raise Exception("Env variable 'OPEN_WEATHER_MAP_API_KEY' is missing!")

    helper = APIHelper(api_key)

    # Airflow variable is created as string
    values = Variable.get("CITIES")
    cities = values.split(',')

    logging.info(f"Get weather of cities: {cities}, type is {type(cities)}")
    logging.info(f"Collecting {NUMBER_OF_WEATHER_REQUEST} times.")

    for index in range(1, NUMBER_OF_WEATHER_REQUEST):
        result = [helper.makeRequest(city) for city in cities]

        now = datetime.now()
        file_name = f'{now.strftime("%Y-%d-%m %H:%M:%S")}.json'
        out = os.path.join(RAW_FILES_DIRECTORY, file_name)
        logging.info(
            f'New file is created with name {out}')
        with open(out, 'w') as outfile:
            json.dump(result, outfile)
        
        # wait a little bit
        time.sleep(2)


def transform_data_into_csv(n_files=None, filename='data.csv'):
    """
    Transforms `json` files into a unique csv file. 
    """
    files = sorted(os.listdir(RAW_FILES_DIRECTORY), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        file = os.path.join(RAW_FILES_DIRECTORY, f)
        if not os.path.isfile(file):
            logging.warn(f'Ignore a not regular file {f}.')
            continue
        if not f.endswith('.json'):
            logging.warn(f'Ignore a not json file {f}.')
            continue

        with open(file, 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)
    logging.info(f'\n {df.head(10)}')

    df.to_csv(os.path.join(CLEAN_DATA_DIRECTORY, filename), index=False)


def train_model_using_lr(task_instance):
    score = get_score_model(kind='lr')
    task_instance.xcom_push(
        key="lr_score",
        value=score
    )


def train_model_using_dt(task_instance):
    score = get_score_model(kind='dt')
    task_instance.xcom_push(
        key="dt_score",
        value=score
    )


def train_model_using_rf(task_instance):
    score = get_score_model(kind='rf')
    task_instance.xcom_push(
        key="rf_score",
        value=score
    )


def train_best_model(task_instance):
    lr_score = task_instance.xcom_pull(
        key="lr_score",
        task_ids="cross_validation_lr"
    )

    dt_score = task_instance.xcom_pull(
        key="dt_score",
        task_ids="cross_validation_dt"
    )

    rf_score = task_instance.xcom_pull(
        key="rf_score",
        task_ids="cross_validation_rf"
    )

    scores = [lr_score, dt_score, rf_score]
    logging.info(f"Scores are: {scores}")
    min_score = min(scores)
    index = scores.index(min_score)

    kinds = ['lr', 'dt', 'rf']
    kind = kinds[index]

    logging.info(f"Best model is {kind}.")
    train_model(kind=kind)


# DAG
dag = DAG(dag_id='airflow_evaluation',
          description="Datascientest Airflow Evaluation",
          tags=['Evaluation', 'datascientest'],
          default_args={'owner': 'airflow',
                        'start_date': days_ago(0, minute=1), },
          schedule_interval=None,
          render_template_as_native_obj=True,
          catchup=False)


# Task 0
init_task = PythonOperator(
    task_id="Start",
    python_callable=init,
    dag=dag
)


# Task 1
weather_task = PythonOperator(
    task_id='get_cities_weather',
    python_callable=get_weather,
    dag=dag
)

init_task >> weather_task

# Task 2


def convert_last_files_data_into_csv():
    transform_data_into_csv(n_files=20)


last_20_files_conversion_task = PythonOperator(
    task_id='convert_last_20_files_into_csv',
    python_callable=convert_last_files_data_into_csv,
    dag=dag
)

# Task 3


def convert_all_data_into_csv():
    transform_data_into_csv(filename='fulldata.csv')


all_files_conversion_task = PythonOperator(
    task_id='convert_all_files_into_csv',
    python_callable=convert_all_data_into_csv,
    dag=dag
)

weather_task >> last_20_files_conversion_task
weather_task >> all_files_conversion_task


# Task 4

cv_lr_task = PythonOperator(
    task_id='cross_validation_lr',
    python_callable=train_model_using_lr,
    dag=dag
)

# Task 4`

cv_dt_task = PythonOperator(
    task_id='cross_validation_dt',
    python_callable=train_model_using_dt,
    dag=dag
)


# Task 4```
cv_rf_task = PythonOperator(
    task_id='cross_validation_rf',
    python_callable=train_model_using_rf,
    dag=dag
)

all_files_conversion_task >> [cv_lr_task, cv_dt_task, cv_rf_task]

# Task 5
train_best_model_task = PythonOperator(
    task_id='train_best_model_task',
    python_callable=train_best_model,
    trigger_rule='all_success',
    dag=dag
)

[cv_lr_task, cv_dt_task, cv_rf_task] >> train_best_model_task
