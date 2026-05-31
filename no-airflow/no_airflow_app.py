from datetime import date

import requests
import pandas as pd
import psycopg2
from clickhouse_driver import Client


def fetch_data_from_github():
    '''Возвращает список с топ-100 репозиториев на GitHub.'''

    GH_REPO_SEARCH_URL = 'https://api.github.com/search/repositories'
    TOP_100_PARAMS = {
        # В параметре q делаем фильтр по минимальному количеству звезд,
        # чтобы в ответе вернулся полный результат (incomplete_results: false).
        'q': 'stars:>20000',
        'sort': 'stars',
        'order': 'desc',
        'per_page': 100,
        'page': 1
    }

    repos = []

    try:
        repos_response = requests.get(
            GH_REPO_SEARCH_URL,
            TOP_100_PARAMS,
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

    except Exception as e:
        raise Exception(f'В ходе запроса к GitHub API произошла ошибка: {e}')

    return repos


def transform_and_load_to_postgres(repos):
    '''Функция трансформации и загрузки данных в Postgres.

    Берет список с топ-100 репозиториев, переносит его в DataFrame
    и загружает DataFrame в таблицу raw_repositories в БД PostgreSQL.
    '''

    extracted_data = []

    try:
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
                    raise KeyError(
                        f'В репозитории отсутствует поле {field}.'
                    )

            if 'login' not in repo['owner']:
                raise KeyError(
                    f'В репозитории {repo["name"]} отсутствует поле login.'
                )

            extracted_data.append({
                'id': repo['id'],
                'name': repo['name'],
                'full_name': repo['full_name'],
                'owner_login': repo['owner']['login'],
                'stargazers_count': repo['stargazers_count'],
                'created_at': repo['created_at']
            })

    except Exception as e:
        raise Exception(
            f'В ходе извлечения данных о репозиториях произошла ошибка: {e}'
        )

    postgres_df = pd.DataFrame(extracted_data)
    postgres_df = postgres_df.dropna(subset=['id'])

    postgres_df['id'] = postgres_df['id'].astype('Int64')
    postgres_df['stargazers_count'] = (
        postgres_df['stargazers_count'].astype('int32')
    )
    postgres_df['created_at'] = (
        pd.to_datetime(postgres_df['created_at'])
    )

    try:
        connection = psycopg2.connect(
            host='localhost',
            port=5434,
            user='postgres',
            password='postgres',
            database='stats_db'
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
                row['created_at']
            )
            for _, row in postgres_df.iterrows()
        ]

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()

    except psycopg2.Error as e:
        if connection:
            connection.rollback()
        raise Exception(f'Ошибка PostgreSQL: {e.pgerror}')

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return


def aggregate_and_push_to_clickhouse():
    '''Функция агрегации данных из Postgres и их загрузки в CH.

    Берет агрегированные данные из таблицы raw_repositories в БД PostgreSQL
    и загружает их в таблицу repo_analytics в БД ClickHouse.
    '''

    try:
        postgres_connection = psycopg2.connect(
            host='localhost',
            port=5434,
            user='postgres',
            password='postgres',
            database='stats_db'
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

        postgres_cursor.execute(select_query)
        aggregated_data = postgres_cursor.fetchall()

        if not aggregated_data:
            raise ValueError(
                'Запрос в БД PostgreSQL вернул пустой результат.'
            )

        clickhouse_client = Client(
            host='localhost',
            port=9000,
            user='default',
            password='default',
            database='default'
        )

        current_date = date.today()

        existing_logins_result = clickhouse_client.execute(
            'SELECT owner_login FROM repo_analytics'
        )
        existing_logins = {row[0] for row in existing_logins_result}

        to_update = []
        to_insert = []

        for record in aggregated_data:
            owner_login, avg_stars_per_repo, total_stars, repo_count = record
            if owner_login in existing_logins:
                to_update.append(record)
            else:
                to_insert.append(record)

        if to_update:
            owner_logins_to_update = [row[0] for row in to_update]
            login_list = ', '.join([f"'{login}'" for login in
                                    owner_logins_to_update])
            delete_query = f'''
            ALTER TABLE repo_analytics DELETE
            WHERE owner_login IN ({login_list})
            '''
            clickhouse_client.execute(delete_query)

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
                    current_date
                )
                for owner_login, avg_stars_per_repo, total_stars,
                repo_count in to_insert
            ]

            clickhouse_client.execute(insert_query, data_to_insert)

    except Exception as e:
        raise Exception(
            'Ошибка в ходе агрегации данных и их загрузки в БД ClickHouse: '
            f'{e}'
        )

    finally:
        if postgres_cursor:
            postgres_cursor.close()
        if postgres_connection:
            postgres_connection.close()
        if clickhouse_client:
            clickhouse_client.disconnect()


if __name__ == '__main__':
    data = fetch_data_from_github()
    print('fetch_data_from_github — OK')
    transform_and_load_to_postgres(data)
    print('transform_and_load_to_postgres — OK')
    aggregate_and_push_to_clickhouse()
    print('aggregate_and_push_to_clickhouse — OK')
