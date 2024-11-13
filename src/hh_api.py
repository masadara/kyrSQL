import requests


def get_vacancies_for_company(company_name):
    """Функция для работы с HH"""
    headers = {"User-Agent": "api-test-agent"}

    vacancies_url = f"https://api.hh.ru/vacancies?text={company_name}"

    response = requests.get(vacancies_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Ошибка при получении вакансий для {company_name}: {response.status_code}"
        )
        return None
