import pandas as pd
import numpy as np
import pymysql
import logging
import datetime
import os
import sqlalchemy
import configparser
from jira import JIRA
from sqlalchemy.dialects.mysql import insert
from mysql_orm import  FeatureDescriptionRow, RequestRelease, RelationRelFeature, \
    RelationSwellFeature, ReleaseRow,  DOR,  DORInformation,   ReleaseDates, ApplicationQuantity, Base, ApplicationStatus
from sqlalchemy import create_engine
from sqlalchemy.orm import  sessionmaker
from sqlalchemy.pool import NullPool
import re
import unicodedata
from sqlalchemy import or_, and_

pymysql.install_as_MySQLdb()


def read_config():
    path = os.path.dirname(os.path.realpath(__file__))
    config = configparser.ConfigParser(interpolation=None)
    # path = r'%s' % os.getcwd().replace('\\', '/')
    myfile = os.path.join(path, 'config.ini')
    print(f"path to config:{myfile}")
    config.read(myfile, encoding='utf-8')
    return config



def start_logging(loggername):
    path = r'%s' % os.getcwd().replace('\\', '/')
    filename = path+f'/{loggername}.log'
    logger = logging.getLogger(loggername)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        fh = logging.FileHandler(filename)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)

    start_time = datetime.datetime.now().replace(microsecond=0)
    logger.info(f'Today: {str(start_time)} starting logging')
    return logger, start_time


def connect_jira():
    config = read_config()
    jira_options = {'server': config['jira.jira_options']['server'],'verify': bool(config['jira.jira_options']['verify'])}
    login = config['jira']['login']
    password = config['jira']['password']
    jira = JIRA(options=jira_options, basic_auth=(login, password), max_retries=1)
    return jira

def create_db_session(mode):
    config = read_config()
    print(config)
    db_engine_param = config['database_' + mode]['db_engine_param']
    local_bind_port = int(config['database_' + mode]['local_bind_port'])
    print(db_engine_param)
    engine = create_engine(db_engine_param % local_bind_port, pool_recycle=3600, pool_pre_ping=True, poolclass=NullPool)
    Session = sessionmaker(bind=engine)
    print(Base.metadata)
    print(engine)
    Base.metadata.create_all(bind = engine)
    return Session, engine


# def create_db_session():
#     config = read_config()
#     db_engine_param = config['database']['db_engine_param']
#     local_bind_port = int(config['database']['local_bind_port'])
#     engine = create_engine(db_engine_param % local_bind_port, pool_recycle=3600, pool_pre_ping=True, poolclass=NullPool)
#     Session = sessionmaker(bind=engine)
#     Base.metadata.create_all(engine)
#     return Session, engine



# Декоратор в случае отсутсвия открытой сессии (на портале она открывается и закрывается в каждом методе)
# открывает ее и закрывает после того, как метод отработает
def inspect_session(func):
    def wrapper(session, *args):
        if not isinstance(session, sqlalchemy.orm.session.Session):
            try:
                session = session()
                return func(session, *args)
            except Exception as ex:
                print(f"Ошибка:{ex}")
            finally:
                session.close()
        else:
            return func(session, *args)
    return wrapper



# @inspect_session
def count_teams_and_apps(session,  release_lst=[]):
    # #     """
    # #     Ф-ия принмает в качестве необязательного параметра список релизов ( ключей или id),
    # #     по которым нужно посчитать количество фичей и команд  и возвращает словарь:
    # #     {"teams_dict":  {'release-key': количество команд для релиза},
    # #      "apps_dict":  {'release-key': {количество инициатив и их статусы для релиза}},
    # #      "release_info": таблица со сгрупированной информацией по каждому релизу}
    # #     Пример использования: res["teams_dict"]['DBIOSCA-113291']
    # #     """

    release_filters = []
    req_filters = []
    rel_filters = []
    swell_filters = []

    try:
        engine = session.get_bind()

        if len(release_lst) > 0:  # могли передать и ключи и id релизов
            release_filters.append(or_(ReleaseRow.jira_key.in_(release_lst), ReleaseRow.release_id.in_(release_lst)))

        df_releases = pd.read_sql(
            session.query(ReleaseRow.release_id, ReleaseRow.jira_key, ReleaseRow.platform).filter(
                *release_filters).statement, engine)
        release_keys = df_releases['jira_key'].to_list()
        platform_lst = list(set(df_releases['platform'].to_list()))
        if len(release_keys) > 0 and len(release_lst) > 0:
            req_filters.append(RequestRelease.release_key.in_(release_keys))
            rel_filters.append(RelationRelFeature.release_key.in_(release_keys))
            swell_filters.append(RelationSwellFeature.swell_key.in_(release_keys))
        df_request_to_release = pd.read_sql(session.query(
            RequestRelease.release_key, RequestRelease.application_id, RequestRelease.status).filter(
            *req_filters).statement, engine)
        df_relation_release_feature = pd.read_sql(session.query(
            RelationRelFeature.release_key, RelationRelFeature.story_key).filter(*rel_filters).statement, engine)
        df_relation_swell_feature = pd.read_sql(session.query(
            RelationSwellFeature.swell_key, RelationSwellFeature.release_key).filter(*swell_filters).statement, engine)
        df_statuses = pd.read_sql(
            session.query(ApplicationStatus.status).filter(ApplicationStatus.platform.in_(platform_lst)).statement,
            engine)
        column_lst = list(set(df_statuses['status'].to_list()))
        column_lst.extend(['release_key', 'applications'])

    except Exception as ex:
        print(ex)
        return {"teams_dict": {}, "apps_dict" : {}, "release_info": pd.DataFrame()}

    df_request_to_release['status'] = df_request_to_release['status'].astype(str)
    df_all_apps = df_request_to_release.groupby(['release_key'], dropna=False).count().reset_index()
    df_apps_statuses = df_request_to_release.groupby(['release_key', 'status'], dropna=False).count().reset_index()
    df_apps_statuses = df_apps_statuses.pivot_table(values='application_id', index=df_apps_statuses.release_key,
                                                    columns='status')

    df_apps_statuses = df_apps_statuses.drop(columns=[col for col in df_apps_statuses if col not in column_lst])
    df_apps_statuses = df_apps_statuses.fillna(0)
    df_apps_statuses = df_apps_statuses.apply(lambda col: col.apply(lambda x: int(x) if type(x) == float else x),
                                              axis=1)
    df_all_apps = df_all_apps[['release_key', 'application_id']]
    df_apps_statuses = pd.merge(df_apps_statuses, df_all_apps, on="release_key", how="inner")
    df_apps_statuses.rename(columns={"application_id": "applications"}, inplace=True)

    df_relation_swell_feature.rename(columns={'release_key': 'story_key', 'swell_key': 'release_key'}, inplace=True)
    df_relations = pd.concat([df_relation_release_feature, df_relation_swell_feature], axis=0)

    df_relations['teams'] = df_relations['story_key'].str.split('-').str[0]
    df_relations = df_relations[['release_key', 'teams']]
    df_relations = df_relations.sort_values(by=['release_key', 'teams'])
    df_relations = df_relations.drop_duplicates(subset=['release_key', 'teams'], keep="first")
    df_teams = df_relations.groupby('release_key', dropna=False).count().reset_index()

    df_release_info = pd.merge(df_apps_statuses, df_teams, on="release_key", how="left")
    # т.к. в бд есть неконсистентность данных проверим корректность ключей релизов
    df_releases.rename(columns={'jira_key': 'release_key'}, inplace=True)
    df_release_info = pd.merge(df_release_info, df_releases, on='release_key', how="inner")

    df_release_info.replace(np.nan, 0, inplace=True)
    df_release_info["teams"] = df_release_info["teams"].astype(int)
    teams_dict = dict(df_teams.values.tolist())

    apps_dict = {}
    for ind, row in df_apps_statuses.iterrows():
        statuses = row.to_dict()
        statuses.pop('release_key')
        apps_dict[row['release_key']] = statuses
    print(f"apps_dict: {apps_dict}")
    return {"teams_dict": teams_dict, "apps_dict": apps_dict, "release_info": df_release_info}

def update_teams_and_apps(session, releases=[]):
    print("we are here")
    try:
        print(session)
        res = count_teams_and_apps(session, releases)
        if res['release_info'].shape[0] > 0:
            print("ура")
            df_release_info = res['release_info']
            df_application_quantity = pd.DataFrame()
            df_application_quantity = df_release_info[['release_id', 'teams', 'applications']]
            df_application_quantity['updated'] = pd.to_datetime('today').date()
            df_application_quantity['updated'] = df_application_quantity['updated'].astype(str)
            df_application_quantity['applications_psi_ready'] = 0
            df_application_quantity['applications_psi_not_ready'] = 0
            df_application_quantity['applications_psi_passed'] = 0
            df_application_quantity['applications_canceled'] = 0
            if 'psi-ready' in df_release_info.columns:
                df_application_quantity['applications_psi_ready'] = df_release_info['psi-ready']
            if 'psi-not-ready' in df_release_info.columns:
                df_application_quantity['applications_psi_not_ready'] = df_release_info['psi-not-ready']
            if 'psi-passed' in df_release_info.columns:
                df_application_quantity['applications_psi_passed'] = df_release_info['psi-passed']
            if 'release' in df_release_info.columns:
                df_application_quantity['applications_psi_passed'] = df_application_quantity[
                                                                         'applications_psi_passed'] + df_release_info[
                                                                         'release']
            if 'canceled' in df_release_info.columns:
                df_application_quantity['applications_canceled'] = df_release_info['canceled']

            on_update_stmt = create_update_stmt(ApplicationQuantity, df_application_quantity)
            session.execute(on_update_stmt)
            session.commit()
        return {"result": 200, "result-text": 'teams and features updated successfully'}
    except Exception as ex:
        print(ex)
        session.rollback()
        return {"result": 500, "result-text": f'teams and features update fail:{ex}'}


def create_update_stmt(tableclass, df_tab):
    cols = list(set(df_tab.columns.to_list()) & set(tableclass.__table__.columns.keys()))
    df_tab = df_tab[cols]
    insert_stmt = insert(tableclass).values(df_tab.to_dict(orient='records'))
    on_update_stmt = insert_stmt.on_duplicate_key_update(
        **{key: insert_stmt.inserted[key] for key in df_tab.columns.to_list()})
    return on_update_stmt


class TextParser():
    @staticmethod
    def normalize_text(text):
        text = unicodedata.normalize("NFKD", text).encode('cp1251', 'ignore').decode('cp1251')
        return text

    @staticmethod
    def remove_tables(text):
        text = re.sub(r"[\r\n]*", "", text)
        text = re.sub("\|\|.*\|\|\|.*\|", "", text)
        return text

    @staticmethod
    def remove_extra_symbols(text):
        text = re.sub('\*', "", text)
        text = ' '.join(text.split())
        return text

    @staticmethod
    def extract_url(text):
        urls = re.findall('[\[]?http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                          text)
        if len(urls) == 1:
            if urls[0][0] == '[' and urls[0][-1] == ']':
                urls[0] = urls[0].lstrip('[')
                urls[0] = urls[0].rstrip(']')
            return urls[0]
        else:
            return ''

    @staticmethod
    def remove_url(text):
        text = re.sub('[\[]?http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', "",
                      text)
        return text



