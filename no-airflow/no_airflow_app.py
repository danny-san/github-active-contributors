import requests
import pandas as pd


def fetch_data_from_github():
    GH_REPO_SEARCH_URL = 'https://api.github.com/search/repositories'
    TOP_100_PARAMS = {
        'q': 'Q',
        'sort': 'stars',
        'order': 'desc',
        'per_page': '100'
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

    return postgres_df


if __name__ == '__main__':
    data = fetch_data_from_github()
    print(transform_and_load_to_postgres(data))
    print('ok')
