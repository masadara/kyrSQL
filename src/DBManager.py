import psycopg2
from psycopg2 import sql


class DBManager:
    """Основной класс для работы с БД"""

    def __init__(self, db_params):
        """Инициализация класса и подключение к базе данных"""
        self.db_params = db_params
        self.create_database()
        self.conn = psycopg2.connect(**db_params)
        self.create_tables()

    def create_database(self):
        """Создает базу данных, если она не существует"""
        db_name = self.db_params["dbname"]

        conn = psycopg2.connect(
            host=self.db_params["host"],
            user=self.db_params["user"],
            password=self.db_params["password"],
        )

        conn.autocommit = True
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
                )
                print(f"База данных '{db_name}' успешно создана.")
            except psycopg2.errors.DuplicateDatabase:
                print(f"База данных '{db_name}' уже существует.")

        conn.close()

    def create_tables(self):
        """Создает таблицы в базе данных, если они не существуют"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    url VARCHAR(255)
                );
            """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    salary_min INT,
                    salary_max INT,
                    currency VARCHAR(10),
                    company_id INT REFERENCES companies(id),
                    url VARCHAR(255)
                );
            """
            )
            self.conn.commit()

    def insert_company(self, name, url):
        """Добавляет новую компанию в таблицу и возвращает её ID"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO companies (name, url) VALUES (%s, %s) RETURNING id",
                (name, url),
            )
            return cursor.fetchone()[0]

    def insert_vacancy(self, title, salary_min, salary_max, currency, company_id, url):
        """Добавляет новую вакансию в таблицу"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO vacancies (title, salary_min, salary_max, currency, company_id, url) VALUES (%s, %s, %s, %s, %s, %s)",
                (title, salary_min, salary_max, currency, company_id, url),
            )
            self.conn.commit()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.name, COUNT(v.id) 
                FROM companies c 
                LEFT JOIN vacancies v ON c.id = v.company_id 
                GROUP BY c.id;
            """
            )
            return cursor.fetchall()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании и ссылки на вакансию"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT v.title, c.name AS company_name, 
                       COALESCE(v.salary_min, 0) AS salary_min, 
                       COALESCE(v.salary_max, 0) AS salary_max,
                       v.url 
                FROM vacancies v 
                JOIN companies c ON v.company_id = c.id;
            """
            )
            return cursor.fetchall()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2) 
                FROM vacancies;
            """
            )
            return cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий с зарплатой выше средней по всем вакансиям"""
        avg_salary = self.get_avg_salary()
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT v.title, c.name AS company_name,
                       COALESCE(v.salary_min, 0) AS salary_min,
                       COALESCE(v.salary_max, 0) AS salary_max,
                       v.url 
                FROM vacancies v 
                JOIN companies c ON v.company_id = c.id
                WHERE (COALESCE(v.salary_min, 0) + COALESCE(v.salary_max, 0)) / 2 > %s;
            """,
                (avg_salary,),
            )
            return cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий с заданным ключевым словом в названии"""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT v.title, c.name AS company_name,
                       COALESCE(v.salary_min, 0) AS salary_min,
                       COALESCE(v.salary_max, 0) AS salary_max,
                       v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                WHERE v.title ILIKE %s;
            """,
                ("%" + keyword + "%",),
            )
            return cursor.fetchall()

    def close(self):
        """Закрывает соединение с базой данных"""
        self.conn.close()
