from src.DBManager import DBManager
from src.hh_api import get_vacancies_for_company

db_params = {
    "dbname": "kyrsql",
    "user": "postgres",
    "password": "563167972268qq",
    "host": "localhost",
    "port": "5432",
}

companies = [
    {"name": "Яндекс"},
    {"name": "Сбер"},
    {"name": "Тинькофф"},
    {"name": "Ростех"},
    {"name": "ВКонтакте"},
    {"name": "Газпром"},
    {"name": "Гринатом"},
    {"name": "ВТБ"},
    {"name": "Альфа-Банк"},
    {"name": "Huawei"},
]


if __name__ == "__main__":
    db_manager = DBManager(db_params)
    for company in companies:
        vacancies_data = get_vacancies_for_company(company["name"])

        if vacancies_data and "items" in vacancies_data:
            company_id = db_manager.insert_company(company["name"], None)

            for vacancy in vacancies_data["items"]:
                title = vacancy.get("name")

                salary_min = (
                    vacancy.get("salary", {}).get("from")
                    if vacancy.get("salary")
                    else None
                )
                salary_max = (
                    vacancy.get("salary", {}).get("to")
                    if vacancy.get("salary")
                    else None
                )
                currency = (
                    vacancy.get("salary", {}).get("currency", "RUR")
                    if vacancy.get("salary")
                    else "RUR"
                )
                url = vacancy.get("alternate_url")

                db_manager.insert_vacancy(
                    title, salary_min, salary_max, currency, company_id, url
                )

    print("Компании и количество вакансий:")
    companies_count = db_manager.get_companies_and_vacancies_count()
    for company in companies_count:
        print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")

    print("\nВсе вакансии:")
    all_vacancies = db_manager.get_all_vacancies()
    for vacancy in all_vacancies:
        print(
            f"Вакансия: {vacancy[0]}, Компания: {vacancy[1]}, Зарплата: {vacancy[2]} - {vacancy[3]}, Ссылка: {vacancy[4]}"
        )

    print("\nСредняя зарплата:", db_manager.get_avg_salary())

    print("\nВакансии с зарплатой выше средней:")
    higher_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
    for vacancy in higher_salary_vacancies:
        print(
            f"Вакансия: {vacancy[0]}, Компания: {vacancy[1]}, Зарплата: {vacancy[2]} - {vacancy[3]}"
        )

    keyword = "python"
    print(f"\nВакансии с ключевым словом '{keyword}':")
    keyword_vacancies = db_manager.get_vacancies_with_keyword(keyword)
    for vacancy in keyword_vacancies:
        print(
            f"Вакансия: {vacancy[0]}, Компания: {vacancy[1]}, Зарплата: {vacancy[2]} - {vacancy[3]}"
        )

    db_manager.close()
