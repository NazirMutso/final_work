from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    'owner': 'nazir_mutso',
    'retries': 0
}

with DAG(default_args=default_args,
    dag_id='home_work',
    description='the finally task',
    start_date=datetime(2023, 7, 22),
    schedule_interval='@daily'
) as dag:
    @task()  # загрузка архива ЕГРЮЛ
    def egrul_downloader():
        import requests
        import logging
        import os
        # проверяем наличие файла в дериктории, чтобы не перезагружать
        if os.access('egrul.json.zip', os.F_OK) == True:
            logging.info('Файл присутствует в директории')
        else:
            logging.error('Файл отсутствует в директории.')
            try:
                url = 'https://ofdata.ru/open-data/download/egrul.json.zip'
                with open('egrul.json.zip', 'wb') as arch:
                    logging.info('Начата загрузка файла')
                    arch.write(requests.get(url).content)
                    logging.info('Архив ЕГРЮЛ загружен')
            except Exception:
                logging.error('Загрузка прервалась. Требуется перезапустить загрузку.')


    @task()  # создание базы данных и таблиц
    def create_tables():
        from sqlalchemy.orm import declarative_base
        from sqlalchemy import create_engine, Column, String, Integer

        engine = create_engine('sqlite:///mt.db', echo=True)

        Base = declarative_base()

        class Table(Base):
            __tablename__ = 'vacancies'
            vacancy_id = Column('vacancy_id', Integer)
            company_names = Column('company_names', String, primary_key=True)
            position = Column('position', String)
            job_description = Column('job_description', String)
            key_skills = Column('key_skills', String)

            def __init__(self, vacancy_id, company_names, position, job_description, key_skills):
                self.vacancy_id = vacancy_id
                self.company_names = company_names
                self.position = position
                self.job_description = job_description
                self.key_skills = key_skills

            def __repr__(self):
                return (
                    f"{self.vacancy_id}, {self.company_names}, {self.position}, {self.job_description}, {self.key_skills}")

        class Table2(Base):
            __tablename__ = 'telecom_companies'
            inn = Column('inn', Integer)
            name = Column('name', String)
            okved = Column('okved', String)
            ogrn = Column('ogrn', Integer, primary_key=True)
            kpp = Column('kpp', Integer)

            def __init__(self, inn, name, okved, ogrn, kpp):
                self.inn = inn
                self.name = name
                self.okved = okved
                self.ogrn = ogrn
                self.kpp = kpp

            def __repr__(self):
                return (f"{self.inn}, {self.name}, {self.okved}, {self.ogrn}, {self.kpp}")

        Base.metadata.create_all(bind=engine)


    @task()  # получение вакансий с api.hh.ru
    def get_vacancies_hh():
        import requests
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session, declarative_base
        import sqlalchemy.exc
        import logging
        import random
        import time
        import pandas as pd

        url = 'https://api.hh.ru/vacancies/'
        db = 'sqlite:///mt.db'

        engine = create_engine(db, echo=True)
        Base = declarative_base(engine)
        # получаем количество вакансий в таблицы
        vacs_num = pd.read_sql_table('vacancies', con=engine)
        # проверяем достаточность вакансий в таблице чтобы не перезапускать task
        if len(vacs_num.index) < 100:
            class Table_v(Base):  # подключаемся к таблице
                __tablename__ = 'vacancies'
                __table_args__ = {'autoload': True}


            def get_ids(url, page):  # создание списка ID вакансий со страницы поиска
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                         'Chrome/114.0.0.0 Safari/537.36'}
                params = {'text': 'meddle python',
                          'search_field': 'name',
                          'area': 113,
                          'per_page': 100,
                          'page': page}
                response = requests.get(url, headers=headers, params=params)
                vacancies = response.json()
                vac_links = [vacancy['id'] for i, vacancy in enumerate(vacancies.get('items'))]
                logging.info('Получен список идентификаторов вакансий')
                return vac_links

            def get_vacs(id):  # получаем данные о вакансии в виде json
                url = f'https://api.hh.ru/vacancies/{id}'
                logging.info(f'Начата загрузка вакансии {url}')
                response = requests.get(url)
                vacancy_json = response.json()
                logging.info(f'Завешена загрузка вакансии {url}')
                return vacancy_json

            flag = len(vacs_num.index)  # чтобы не грузить лишние вакансии
            page = 0  # страница поиска
            while flag < 10:
                logging.info(f'Парсится страница поиска №{page}')
                try:
                    ids = get_ids(url, page)  # проверка на наличие вакансий на странице поиска
                    for id in ids:
                        result = get_vacs(id)
                        try:
                            # проверка на наличие ключевых навыков в вакансии
                            if 'key_skills' in result.keys() and result['key_skills']:
                                skills_list = [skill['name'] for skill in result['key_skills']]
                                # подготавливаем данные для записи
                                new_vac = Table_v(vacancy_id=int(result['id']),
                                                  company_names=result['employer']['name'],
                                                  position=result['name'],
                                                  job_description=result['description'],
                                                  key_skills=','.join(skills_list))
                                with Session(engine) as session:
                                    session.add(new_vac)
                                    session.commit()
                                    flag += 1  # добавляем к счетчику вакансий
                                    logging.info(f'Вакансия {flag} записана в таблицу')
                            else:
                                # если не указаны ключевые навыки, переходим к след. вакансии
                                logging.info(f'Не указаны ключевые навыки в вакансии')
                                continue
                        except sqlalchemy.exc.IntegrityError:
                            # дубликат, бывают при поиске по России, одна вакансия указывается в разных регионах
                            logging.error(f'Вакансия компании "{result["employer"]["name"]}" уже есть в таблице')
                        if flag == 100:  # условие на остановку цикла
                            logging.info(f'Записано {flag} вакансий. Программа выполнена')
                            break
                    time.sleep(random.randrange(6, 8))
                    page += 1
                except TypeError:
                    logging.error('Выгружены все подходящие вакансии. Пустая страница поиска')
                    logging.info(f'Получено {flag} вакансий')
                    break
        else:
            logging.info('Вакансии загружены в базу ранее')


    @task()  # получение данных о телеком компаниях из архива ЕГРЮЛ
    def get_companies_61():
        import pandas as pd
        import zipfile
        import logging
        from sqlalchemy import create_engine
        from sqlalchemy.orm import declarative_base, Session
        import sqlalchemy.exc
        import os

        # проверка на наличие архива в директории
        if os.access('egrul.json.zip', os.F_OK):
            engine = create_engine('sqlite:///mt.db', echo=True)
            companies_num = pd.read_sql_table('telecom_companies', con=engine)
            # проверка на наличие данных в БД, чтобы не повторять скрипт
            if len(companies_num.index) < 20914:
                Base = declarative_base(engine)

                class Table(Base):
                    __tablename__ = 'telecom_companies'
                    __table_args__ = {'autoload': True}

                with zipfile.ZipFile('egrul.json.zip', 'r') as arch:
                    file_names = arch.namelist()
                    for file in file_names:
                        df = pd.read_json(arch.open(file))
                        logging.info(f'Обрабатывается файл: {file}')
                        for i in range(len(df)):
                            # проверяем наличие ОКВЭД у компании
                            if 'СвОКВЭД' in df['data'][i] and 'СвОКВЭДОсн' in df['data'][i]['СвОКВЭД']:
                                # берем только ОКВЭД 61 и компании с ИНН
                                if df['data'][i]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][:2] == '61' and df['inn'][
                                    i] != '':
                                    logging.info(f'Компания "{df["name"][i]}" соответствует ОКВЭД 61')
                                    # подготавливаем данные к записи
                                    new_comp = Table(inn=int(df['inn'][i]),
                                                     name=df['name'][i],
                                                     okved=df['data'][i]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                                                     ogrn=int(df['ogrn'][i]),
                                                     kpp=int(df['kpp'][i]))
                                    try:
                                        with Session(engine) as session:
                                            session.add(new_comp)
                                            session.commit()
                                            logging.info(f'Данные о компании "{df["name"][i]}" записаны в базу')
                                    except sqlalchemy.exc.IntegrityError:
                                        # отлов дубликатов
                                        logging.error(f'Компания "{df["name"][i]}" уже есть в базе')
                                else:
                                    logging.error(f'У компании "{df["name"][i]}" не подходящий ОКВЭД')
                            else:
                                logging.error(f'У компании "{df["name"][i]}" отсутствует ОКВЭД')
                logging.info('Архив отработан. Компании записаны в базу')
            else:
                logging.info('Телеком компании получены ранее')
        else:
            logging.error('Архив ЕГРЮЛ отсутствует в директории')

    @task()  # вывод топ 10 скиллов
    def get_top_skills():
        import pandas as pd
        import logging
        from sqlalchemy import create_engine

        engine = create_engine('sqlite:///mt.db', echo=True)
        tele_comps = pd.read_sql_table('telecom_companies', con=engine)
        hh_vacs = pd.read_sql_table('vacancies', con=engine)

        if len(tele_comps.index) > 0 and len(hh_vacs.index) > 0:
            skills_list = []
            skills_set = set()
            for vac in hh_vacs['company_names']:
                for comp in tele_comps['name']:
                    if vac.upper() in comp:
                        skills_set.update(
                            str(hh_vacs.loc[hh_vacs['company_names'] == vac]['key_skills'].item()).split(','))
                        skills_list += str(hh_vacs.loc[hh_vacs['company_names'] == vac]['key_skills'].item()).split(',')
                        break
            count_skills = {k: skills_list.count(k) for k in skills_set}
            top_skills = dict(sorted(count_skills.items(), key=lambda x: x[1], reverse=True))
            print(*list(top_skills.items())[:10], sep='\n')
        else:
            logging.info('Нет данных в базе для работы!')


    egrul_downloader() >> create_tables() >> [get_companies_61(), get_vacancies_hh()] >> get_top_skills()