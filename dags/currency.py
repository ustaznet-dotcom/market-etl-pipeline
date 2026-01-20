import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator  # для создания задач в Airflow
from clickhouse_driver import Client  # для подключения к ClickHouse


# Устанавливаем URL для API и начальную дату для извлечения данных
URL = 'https://api.exchangerate.host/timeframe'  # URL для получения курсов валют из API

dag = DAG('currency_etl_dag',
          schedule_interval='@daily',
          start_date=days_ago(1))
# Настройка подключения к базе данных ClickHouse
CH_CLIENT = Client(
    host='**',  # IP-адрес сервера ClickHouse
    user='student',  # Имя пользователя для подключения
    password='dfqh89fhq8',  # Пароль для подключения
    database='sandbox'  # База данных, к которой подключаемся
)


# Функция для извлечения данных с API и сохранения их в локальный файл
def extract_data(url, j_file):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    params = {
    "access_key": '043dc9dad696914726d3064e9d917294',
    "source": 'USD',
    "start_date": '2023-01-01',
    "end_date": '2023-01-01'
}
    response = req.get(url=url, params=params)
    data = response.json()  # ← получаем словарь
    with open(j_file, 'w', encoding='utf-8') as f:
      json.dump(data, f, indent=2)


# Функция для обработки данных в формате JSON и преобразования их в CSV
def transform_data(j_file, csv_file):
    """
    Эта функция обрабатывает полученные данные в формате JSON
    и преобразует их в табличном формате для дальнейшей работы.
    В конце данные записываются в CSV файл
    """
    with open(j_file, 'r', encoding='utf-8') as f:
      data = json.load(f)

    date = list(data['quotes'].keys())[0]
    source = data['source']

    # Создаем список всех строк
    rows = []
    for currency_pair, rate in data['quotes'][date].items():
      rows.append({
            'date': date,
            'currency_source': source,
            'currency': currency_pair.replace(source, ""),
            'value': rate
        })

    # Создаем DataFrame из всего списка сразу
    df = pd.DataFrame(rows)
    df.to_csv(csv_file, sep=",", encoding="utf-8", index=False)


# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client):
    """
    Эта функция считывает CSV файл, создает таблицу в
    базе данных ClickHouse и добавляет данные в неё
    """
    data_frame = pd.read_csv(csv_file)

    client.execute(f'create table if not exists {table_name} (date String, currency_source String, \
    currency String, value Float64) ENGINE Log')

    client.execute(f'insert into {table_name} values', data_frame.to_dict('records'))

extract_dt = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    op_kwargs={'url': URL, 'j_file': 'umer_currency.json'},
    dag=dag
)

transform_dt = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    op_kwargs={'j_file': 'umer_currency.json',
               'csv_file': 'umer_currency.csv'},
    dag=dag
    )

upload_to_click = PythonOperator(
     task_id='upload_to_clickhouse_task',
     python_callable=upload_to_clickhouse,
     op_kwargs={'csv_file': 'umer_currency.csv',
                'table_name': 'umer_currency_data',
                'client': CH_CLIENT},
     dag=dag
)

extract_dt >> transform_dt >> upload_to_click