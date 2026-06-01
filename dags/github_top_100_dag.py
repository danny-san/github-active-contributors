import logging
from datetime import datetime, timedelta, date

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from clickhouse_driver import Client


logger = logging.getLogger(__name__)

# Конфигурация подключений из переменных Airflow
POSTGRES_HOST = 'postgres_db'
POSTGRES_PORT = 5432
POSTGRES_DB = 'stats_db'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'

CLICKHOUSE_HOST = 'clickhouse_server'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'default'
CLICKHOUSE_DATABASE = 'default'


def fetch_data_from_github(**context):
    '''
    Задача 1: Получение топ-100 репозиториев из GitHub API.
    '''
    GH_REPO_SEARCH_URL = 'https://api.github.com/search/repositories'
    TOP_100_PARAMS = {
        'q': 'stars:>20000',
        'sort': 'stars',
        'order': 'desc',
        'per_page': 100,
        'page': 1,
    }

    try:
        logger.info('Начинаю запрос к GitHub API...')
        repos_response = requests.get(
            GH_REPO_SEARCH_URL,
            params=TOP_100_PARAMS,
            timeout=30,
            headers={'Accept': 'application/vnd.github.v3+json'},
        )
        repos_response.raise_for_status()

        if not repos_response.text:
            raise ValueError('GitHub API вернул пустой ответ.')

        data = repos_response.json()

        if 'items' not in data:
            raise KeyError('Ключ "items" отсутствует в ответе.')

        repos = data['items']

        if not isinstance(repos, list):
            raise TypeError('Под ключом "items" лежит не список.')

        if len(repos) == 0:
            raise ValueError('Найдено 0 репозиториев.')

        logger.info(f'Успешно получено {len(repos)} репозиториев')

        # Сохраняем данные в XCom для передачи следующей задаче
        context['task_instance'].xcom_push(key='repos_data', value=repos)

        return f'Успешно получено {len(repos)} репозиториев'

    except Exception as e:
        error_msg = f'Ошибка при запросе к GitHub API: {e}'
        logger.error(error_msg)
        raise AirflowException(error_msg)


def transform_and_load_to_postgres(**context):
    '''
    Задача 2: Трансформация данных и загрузка в PostgreSQL.
    '''
    # Получаем данные из XCom от предыдущей задачи
    ti = context['task_instance']
    repos = ti.xcom_pull(key='repos_data', task_ids='fetch_data_from_github')

    if not repos:
        raise AirflowException('Нет данных для загрузки в PostgreSQL')

    extracted_data = []

    try:
        logger.info('Начинаю обработку данных репозиториев...')

        for repo in repos:
            required_fields = [
                'id',
                'name',
                'full_name',
                'owner',
                'stargazers_count',
                'created_at',
            ]
            for field in required_fields:
                if field not in repo:
                    raise KeyError(f'В репозитории отсутствует поле {field}.')

            if 'login' not in repo['owner']:
                raise KeyError(
                    f'В репозитории {repo["name"]} отсутствует поле login.'
                )

            extracted_data.append(
                {
                    'id': repo['id'],
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'owner_login': repo['owner']['login'],
                    'stargazers_count': repo['stargazers_count'],
                    'created_at': repo['created_at'],
                }
            )

        logger.info(f'Обработано {len(extracted_data)} репозиториев')

    except Exception as e:
        error_msg = f'Ошибка при извлечении данных: {e}'
        logger.error(error_msg)
        raise AirflowException(error_msg)

    # Создаем DataFrame
    postgres_df = pd.DataFrame(extracted_data)
    postgres_df = postgres_df.dropna(subset=['id'])

    postgres_df['id'] = postgres_df['id'].astype('Int64')
    postgres_df['stargazers_count'] = postgres_df[
        'stargazers_count'
    ].astype('int32')
    postgres_df['created_at'] = pd.to_datetime(postgres_df['created_at'])

    connection = None
    cursor = None

    try:
        logger.info(
            f'Подключаюсь к PostgreSQL {POSTGRES_HOST}:{POSTGRES_PORT}...'
        )
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
        )
        cursor = connection.cursor()

        insert_query = '''
        INSERT INTO raw_repositories
        (id, name, full_name, owner_login, stargazers_count, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            stargazers_count = EXCLUDED.stargazers_count
        '''

        data_to_insert = [
            (
                row['id'],
                row['name'],
                row['full_name'],
                row['owner_login'],
                row['stargazers_count'],
                row['created_at'],
            )
            for _, row in postgres_df.iterrows()
        ]

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()

        logger.info(
            f'Успешно загружено {len(data_to_insert)} записей в PostgreSQL'
        )

        return f'Загружено {len(data_to_insert)} записей в PostgreSQL'

    except psycopg2.Error as e:
        if connection:
            connection.rollback()
        error_msg = f'Ошибка PostgreSQL: {e.pgerror}'
        logger.error(error_msg)
        raise AirflowException(error_msg)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def aggregate_and_push_to_clickhouse(**context):
    '''
    Задача 3: Агрегация данных из PostgreSQL и загрузка в ClickHouse
    '''
    postgres_connection = None
    postgres_cursor = None
    clickhouse_client = None

    try:
        logger.info('Подключаюсь к PostgreSQL для агрегации...')
        postgres_connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
        )
        postgres_cursor = postgres_connection.cursor()

        select_query = '''
        SELECT
            owner_login,
            AVG(stargazers_count) as avg_stars_per_repo,
            SUM(stargazers_count) as total_stars,
            COUNT(*) as repo_count
        FROM raw_repositories
        GROUP BY owner_login
        '''

        logger.info('Выполняю агрегацию данных...')
        postgres_cursor.execute(select_query)
        aggregated_data = postgres_cursor.fetchall()

        if not aggregated_data:
            raise ValueError('Запрос в PostgreSQL вернул пустой результат.')

        logger.info(f'Получено {len(aggregated_data)} агрегированных записей')

        logger.info(
            f'Подключаюсь к ClickHouse {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...'
        )
        clickhouse_client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

        current_date = date.today()

        # Получаем существующие записи
        existing_logins_result = clickhouse_client.execute(
            'SELECT owner_login FROM repo_analytics'
        )
        existing_logins = {row[0] for row in existing_logins_result}
        logger.info(
            f'Найдено {len(existing_logins)} существующих записей в ClickHouse'
            )

        to_update = []
        to_insert = []

        for record in aggregated_data:
            owner_login, avg_stars_per_repo, total_stars, repo_count = record
            if owner_login in existing_logins:
                to_update.append(record)
            else:
                to_insert.append(record)

        logger.info(
            f'Будет обновлено: {len(to_update)}, вставлено: {len(to_insert)}'
        )

        # Для ClickHouse используем INSERT с обработкой дубликатов
        if to_update:
            # Удаляем старые записи
            owner_logins_to_update = [row[0] for row in to_update]
            login_list = ', '.join(
                [f"'{login}'" for login in owner_logins_to_update]
            )
            delete_query = f'''
            ALTER TABLE repo_analytics DELETE
            WHERE owner_login IN ({login_list})
            '''
            logger.info(
                f'Удаляю старые записи для {len(to_update)} пользователей'
            )
            clickhouse_client.execute(delete_query)

            # Добавляем обновленные данные в список на вставку
            to_insert.extend(to_update)

        if to_insert:
            insert_query = '''
            INSERT INTO repo_analytics (owner_login, avg_stars_per_repo,
            total_stars, repo_count, updated_at)
            VALUES
            '''

            data_to_insert = [
                (
                    owner_login,
                    float(avg_stars_per_repo),
                    total_stars,
                    repo_count,
                    current_date,
                )
                for owner_login, avg_stars_per_repo, total_stars,
                repo_count in to_insert
            ]

            logger.info(f'Загружаю {len(data_to_insert)} записей в ClickHouse')
            clickhouse_client.execute(insert_query, data_to_insert)

        logger.info('Данные успешно загружены в ClickHouse')

        return (
            f'Загружено {len(to_insert)} записей в ClickHouse (обновлено: '
            f'{len(to_update)}, новых: {len(to_insert) - len(to_update)})'
        )

    except Exception as e:
        error_msg = f'Ошибка при агрегации и загрузке в ClickHouse: {e}'
        logger.error(error_msg)
        raise AirflowException(error_msg)

    finally:
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_connection:
            postgres_connection.close()
        if clickhouse_client:
            clickhouse_client.disconnect()


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_stats_pipeline',
    default_args=default_args,
    description='DAG для сбора статистики GitHub репозиториев',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['github', 'postgres', 'clickhouse'],
)

# Создание задач
fetch_task = PythonOperator(
    task_id='fetch_data_from_github',
    python_callable=fetch_data_from_github,
    provide_context=True,
    dag=dag,
)

transform_load_task = PythonOperator(
    task_id='transform_and_load_to_postgres',
    python_callable=transform_and_load_to_postgres,
    provide_context=True,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_and_push_to_clickhouse',
    python_callable=aggregate_and_push_to_clickhouse,
    provide_context=True,
    dag=dag,
)

fetch_task >> transform_load_task >> aggregate_task
