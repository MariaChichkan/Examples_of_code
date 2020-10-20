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
    RelationSwellFeature, ReleaseRow,  DOR,  DORInformation,   ReleaseDates, ApplicationQuantity, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import  sessionmaker
from sqlalchemy.pool import NullPool
import re
import unicodedata

pymysql.install_as_MySQLdb()


def read_config():
    config = configparser.ConfigParser(interpolation=None)
    path = r'%s' % os.getcwd().replace('\\', '/')
    myfile = os.path.join(path, 'config.ini')
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



def create_db_session():
    config = read_config()
    db_engine_param = config['database']['db_engine_param']
    local_bind_port = int(config['database']['local_bind_port'])
    engine = create_engine(db_engine_param % local_bind_port, pool_recycle=3600, pool_pre_ping=True, poolclass=NullPool)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session, engine



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

@inspect_session
def count_teams_and_features(session,  release_lst=[]):
    # #     """
    # #     Ф-ия принмает в качестве необязательного параметра список релизов,
    # #     по которым нужно посчитать количество фичей и команд  и возвращает словарь:
    # #     {"count_team":  {'release-key': количество команд для релиза},
    # #      "count_features":  {'release-key': количество фичей для релиза},
    # #      "release_info": таблица со сгрупированной информацией по каждому релизу}
    # #     Пример использования: res["count_team"]['DBIOSCA-113291']
    # #     """
    release_filters = []
    req_filters = []
    rel_filters = []
    swell_filters = []
    if len(release_lst) > 0:
        release_filters.append(ReleaseRow.jira_key.in_(release_lst))
        req_filters.append(RequestRelease.release_key.in_(release_lst))
        rel_filters.append(RelationRelFeature.release_key.in_(release_lst))
        swell_filters.append(RelationSwellFeature.swell_key.in_(release_lst))
    try:
        engine = session.get_bind()
        df_releases = pd.read_sql(
            session.query(ReleaseRow.release_id, ReleaseRow.jira_key).filter(*release_filters).statement, engine)
        df_request_to_release = pd.read_sql(session.query(
            RequestRelease.release_key, RequestRelease.feature_id).filter(*req_filters).statement, engine)
        df_relation_release_feature = pd.read_sql(session.query(
            RelationRelFeature.release_key, RelationRelFeature.story_key).filter(*rel_filters).statement, engine)
        df_relation_swell_feature = pd.read_sql(session.query(
            RelationSwellFeature.swell_key, RelationSwellFeature.release_key).filter(*swell_filters).statement, engine)
    except Exception as ex:
        print(ex)
        return {"teams_dict": {}, "features_dict": {}, "release_info": pd.DataFrame()}

    df_features = df_request_to_release.groupby('release_key').count().reset_index()
    df_relation_swell_feature.rename(columns={'release_key': 'story_key', 'swell_key': 'release_key'}, inplace=True)
    df_relations = pd.concat([df_relation_release_feature, df_relation_swell_feature], axis=0)

    df_relations['teams'] = df_relations['story_key'].str.split('-').str[0]
    df_relations = df_relations[['release_key', 'teams']]
    df_relations = df_relations.sort_values(by=['release_key', 'teams'])
    df_relations = df_relations.drop_duplicates(subset=['release_key', 'teams'], keep="first")
    df_teams = df_relations.groupby('release_key').count().reset_index()

    df_release_info = pd.merge(df_features, df_teams, on="release_key", how="left")
    # т.к. в бд есть неконсистентность данных проверим корректность ключей релизов
    df_release_info = pd.merge(df_release_info, df_releases, left_on='release_key', right_on='jira_key', how="inner")

    df_release_info.replace(np.nan, 0, inplace=True)
    df_release_info["teams"] = df_release_info["teams"].astype(int)
    df_release_info.rename(columns={"feature_id": "features"}, inplace=True)
    teams_dict = dict(df_release_info[['release_key', "teams"]].values.tolist())
    features_dict = dict(df_release_info[['release_key', "features"]].values.tolist())
    df_release_info = df_release_info[['release_id', 'features', 'teams']]
    return {"teams_dict": teams_dict, "features_dict": features_dict, "release_info": df_release_info}



def create_update_stmt(tableclass, df_tab):
    cols = list(set(df_tab.columns.to_list()) & set(tableclass.__table__.columns.keys()))
    df_tab = df_tab[cols]
    insert_stmt = insert(tableclass).values(df_tab.to_dict(orient='records'))
    on_update_stmt = insert_stmt.on_duplicate_key_update(
        **{key: insert_stmt.inserted[key] for key in df_tab.columns.to_list() if key in tableclass.__table__.columns.keys()})
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