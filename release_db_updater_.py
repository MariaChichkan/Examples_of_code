from sqlalchemy.inspection import inspect
import pandas as pd
import sshtunnel
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.orm import mapper, sessionmaker
import datetime
import numpy as np
import sys
import uuid

from mysql_orm import  FeatureDescriptionRow, RequestRelease, RelationRelFeature, \
    RelationSwellFeature, ReleaseRow,  DOR,  DORInformation,   ReleaseDates, Base


class DBConnector:
    def __init__(self, Base, logger, start_time=""):
        print(f'start_time in DBConnector:{start_time}')
        self.logger = logger
        self.start_time = start_time
        print(f'self.start_time in init:{self.start_time}')
        self.tunnel = self.open_tunnel()
        self.engine = self.create_db_engine(Base)
        self.session = self.create_db_session()
        
    def open_tunnel(self):
        sshtunnel.SSH_TIMEOUT = 20.0
        sshtunnel.TUNNEL_TIMEOUT = 20.0
        ssh_pass = "pass"
        ssh_username = "uname"

        tunnel = sshtunnel.SSHTunnelForwarder(
            ('ssh.pythonanywhere.com'),
            ssh_username=ssh_username,
            ssh_password=ssh_pass,
            remote_bind_address=(
                 'MariaChichkan.mysql.pythonanywhere-services.com', 3306)
        )
        try:
            tunnel.start()
            return tunnel
        except Exception as ex:
            self.logger.error(f'__DBConnector__Error: {str(ex)} while starting tunnel to DB')

    def stop_tunnel(self):
        print(f' self.start_time:{self.start_time}')
        if self.start_time !="":
            update_time = datetime.datetime.now().replace(microsecond=0) - self.start_time
            self.logger.info(
                f'__DBConnector__Info: End of DB updating. Update time: {str(update_time)}')
        else:
            self.logger.info(f'__DBConnector__Info: {str(datetime.datetime.now().replace(microsecond=0))} End of DB updating.')
        self.session.bind.dispose()
        self.tunnel.stop()


    def check_connection(self):
        if not self.tunnel.is_alive:
            self.tunnel.restart()

    def create_db_engine(self, Base):

        db_engine_param = 'mysql://MariaChichkan:passw@127.0.0.1:%s/MariaChichkan$release_portal?charset=utf8'
        try:
            engine = create_engine(db_engine_param % self.tunnel.local_bind_port, pool_recycle=280, echo=False)
            Base.metadata.create_all(engine)
            return engine
        except Exception as ex:
            self.logger.error(f'__DBConnector__Error: {str(ex)} white creating DB engine')
    
    def create_db_session(self):
        try:
            session = sessionmaker(bind=self.engine)()
            return session
        except Exception as ex:
            self.logger.error(f'__DBConnector__Error: {str(ex)} while creating DB session')
        
        

    def insert_db_data(self, tableclass, df_tab, update=False):
        tabname = tableclass.__tablename__
        if df_tab.shape[0] == 0:
            self.logger.info(f'__DBConnector__Info: no new rows to insert to table {tabname}')
            return
        try:
            self.check_connection()
            df_db_tab = pd.read_sql(tabname, con=self.engine)
            df_db_tab.columns = [x + '_db' for x in df_db_tab.columns]
            primary_keys = [key.name for key in inspect(tableclass).primary_key if key.autoincrement != True]
            primary_keys_auto = [key.name for key in inspect(tableclass).primary_key if key.autoincrement == True]
            primary_keys_db = [x + '_db' for x in primary_keys]
            primary_keys_auto_db = [x + '_db' for x in primary_keys_auto]
            columns = [key.name for key in inspect(tableclass).columns if key.autoincrement != True]

            df_tab_to_update = pd.merge(df_tab, df_db_tab, left_on=primary_keys, right_on=primary_keys_db, how="inner")
            df_tab_to_update = df_tab_to_update[columns + primary_keys_auto_db]
            for key in primary_keys_auto_db:
                df_tab_to_update.rename(columns={key: key.rsplit('_db')[0]}, inplace=True)
            df_tab_to_insert = pd.merge(df_tab, df_db_tab, left_on=primary_keys, right_on=primary_keys_db, how="left")
            df_tab_to_insert = df_tab_to_insert[pd.isna(df_tab_to_insert[primary_keys_db[0]]) == True]
            df_tab_to_insert = df_tab_to_insert[columns]

            if df_tab_to_update.shape[0] > 0 and update:
                self.session.bulk_update_mappings(tableclass, df_tab_to_update.to_dict(orient='records'))
                self.session.commit()
                self.logger.info(f'__DBConnector__Info: updated {df_tab_to_update.shape[0]} rows in {tabname}')
            elif  df_tab_to_update.shape[0] == 0 and update:
                self.logger.info(f'__DBConnector__Info: no new rows to update {tabname}')
            if df_tab_to_insert.shape[0] > 0:
                df_tab_to_insert.to_sql(tabname, self.engine, if_exists='append', index=False)
                self.logger.info(f'__DBConnector__Info: inserted {df_tab_to_insert.shape[0]} rows to {tabname}')
            elif  df_tab_to_insert.shape[0] == 0:
                self.logger.info(f'__DBConnector__Info: no new rows to insert to {tabname}')

        except Exception as ex:
            print(ex)
            self.logger.error(f'__DBConnector__Error: {str(ex)} while inserting data to {tabname}')
            self.session.rollback()
            self.stop_tunnel()
            sys.exit(-1)

    def get_db_data(self, tabname, query_params={}):
        params_str = ""
        params_lst = []
        df_tab = pd.DataFrame()
        for key, value in query_params.items():
            if isinstance(value, list):
                value = [f'"{str(i)}"' if isinstance(i, str) else str(i) for i in value]
                value_str = ','.join(value)
            else:
                value_str = f'"{str(value)}"' if isinstance(value, str) else str(value)
            params_lst.append(f'{key} IN ({value_str})')
        params_str = ' AND '.join(params_lst)
        if params_str:
            params_str = f' WHERE {params_str}'

        sql_query = f'SELECT * FROM `{tabname}`{params_str}'
        try:
            self.check_connection()
            df_tab = pd.read_sql(sql_query, con=self.engine)
            return df_tab
        except Exception as ex:
            self.logger.error(f'__DBConnector__Error {str(ex)} while getting data from {tabname}')
            self.stop_tunnel()
            sys.exit(-1)


class UpdateReleaseDB(DBConnector):
    def __init__(self, Base, logger, start_time=""):
        super().__init__(Base, logger, start_time)


    @staticmethod
    def generate_id(id):
        id = str(uuid.uuid4())
        return id

    def update_ReleaseRow(self, project_data, RELEASE_TYPE):
        #         RELEASE_TYPE = 'epics'
        RELEASE_ID = 'id_' + RELEASE_TYPE
        RELEASE_KEY = 'key_' + RELEASE_TYPE
        RELEASE_LABEL = 'fields.labels_' + RELEASE_TYPE
        RELEASE_SUMMARY = 'fields.summary_' + RELEASE_TYPE

        df_new_releases = pd.DataFrame()
        df_releases = project_data[
            [RELEASE_ID, RELEASE_KEY, RELEASE_LABEL, RELEASE_SUMMARY]]
        df_releases = df_releases.drop_duplicates(subset=RELEASE_ID, keep="first")
        df_releases.rename(columns={RELEASE_ID: 'release_id', RELEASE_SUMMARY: 'name', RELEASE_LABEL: 'platform',
                                    RELEASE_KEY: 'jira_key'}, inplace=True)
        self.insert_db_data(ReleaseRow, df_releases)



        # Создадим feature_id для тех key_releases-key_stories-platform, для которых их еще нет
    def update_FeatureDescriptionRow(self, project_data, name_from, release_type):
        application_keys = list(set(project_data['key_applications'].to_list()))
        # Выберем по application_key данные по feature id и application id
        db_requestrelease = self.get_db_data('requests_to_release',
                                                   query_params={'application_key': application_keys})
        project_data = pd.merge(project_data, db_requestrelease,
                                left_on=['key_applications', f'fields.labels_{release_type}',
                                         f'key_{release_type}'],
                                right_on=['application_key', 'platform', 'release_key'], how="left")
        # Для application key ( инициатив) для которых не нашлось аписей в БД необходимо создать сгенерировать новые feature id
        df_features_to_create = project_data[pd.isna(project_data['application_id']) == True]
        df_features_to_create = df_features_to_create.sort_values(by='key_applications')
        df_features_to_create = df_features_to_create.drop_duplicates(subset='key_applications', keep="first")
        df_features_to_create['feature_id'] = df_features_to_create['feature_id'].apply(self.generate_id)
        df_features_for_app = df_features_to_create[['key_applications', 'feature_id']]
        df_features_for_app.rename(columns={'feature_id': 'feature_id_new'}, inplace=True)
        project_data = pd.merge(project_data, df_features_for_app, on='key_applications', how="left")
        project_data['feature_id'] = np.where(pd.isna(project_data['feature_id']) == True,
                                              project_data['feature_id_new'], project_data['feature_id'])
        # Сохраним новые feature id в БД
        df_features_to_create = df_features_to_create[
            [f'fields.summary_{name_from}', 'feature_id', 'fields.created_applications',
             'fields.updated_applications']]
        df_features_to_create.rename(
            columns={'fields.created_applications': 'created', 'fields.updated_applications': 'updated',
                     f'fields.summary_{name_from}': 'name'}, inplace=True)
        df_features_to_create['user'] = ''
        self.insert_db_data(FeatureDescriptionRow, df_features_to_create)
        return project_data

    def update_release_mp_web(self, project_data):
        # Получим данные которые уже есть по application_key (для проверки по ним корректности релиза)
        application_key_lst = list(set(project_data['key_applications']))
        if len(application_key_lst) == 0:
            return #нечего обновлять
        df_releases = self.get_db_data('requests_to_release', query_params={'application_key': application_key_lst})
        df_releases.rename(columns={'release_id': 'old_release_id', 'release_key': 'old_release_key' },inplace=True )
        df_releases_to_update = pd.merge(project_data, df_releases, left_on='key_applications',
                                         right_on='application_key',  how="inner")
        df_releases_to_update = df_releases_to_update[df_releases_to_update['id_releases'] !=
                                                      df_releases_to_update['old_release_id']]
        df_releases_to_update = df_releases_to_update[['application_id', 'application_key', 'feature_id', 'platform',
                                                       'id_releases', 'key_releases', 'segment', 'request_type']]

        df_releases_to_update = df_releases_to_update.sort_values(by='application_key')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_key', keep="first")
        df_releases_to_update.rename(columns={'id_releases':'release_id',  'key_releases': 'release_key'}, inplace=True)
        if df_releases_to_update.shape[0] > 0:
            # print(f'Перепривязали релизы у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
            self.logger.info(f'__UpdateReleaseDB__Info Перепривязали релизы у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)
        df_releases_to_update = df_releases_to_update[
            ['application_id', 'application_key', 'release_id', 'release_key']]
        application_id_lst = list(set(df_releases_to_update['application_id'].to_list()))
        if len(application_id_lst) == 0:
            return #нечего обновлять
        df_rel_feat = self.get_db_data('relation_release_feature',
                                                query_params={'application_id': application_id_lst})
        df_rel_feat.rename(columns={'release_key': 'old_release_key'}, inplace=True)
        df_rel_feat_to_update = pd.merge(df_releases_to_update, df_rel_feat, on='application_id', how="inner")
        df_rel_feat_to_update = df_rel_feat_to_update[df_rel_feat_to_update['release_key']
                                                      != df_rel_feat_to_update['old_release_key']]

        df_rel_feat_to_update = df_rel_feat_to_update[['application_id', 'platform', 'feature_name', 'release_key',
                                                       'story_key', 'type', 'result', 'error_code',
             'last_update']]
        if df_rel_feat_to_update.shape[0]>0:
            self.logger.info(
                f'__UpdateReleaseDB__Info Перепривязали релизы у {df_rel_feat_to_update.shape[0]} инициатив в RelationRelFeature')
            # print(f'Перепривязали релизы у {df_rel_feat_to_update.shape[0]} инициатив в RelationRelFeature')
            self.insert_db_data(RelationRelFeature, df_rel_feat_to_update)


    def update_release_ufs(self, project_data):
        # Получим данные которые уже есть по application_key (для проверки по ним корректности релиза)
        application_key_lst = list(set(project_data['key_applications']))
        if len(application_key_lst) == 0:
            return #нечего обновлять
        df_releases = self.get_db_data('requests_to_release', query_params={'application_key': application_key_lst})
        df_releases.rename(columns={'release_id': 'old_release_id', 'release_key': 'old_release_key'}, inplace=True )
        df_releases_to_update = pd.merge(project_data, df_releases, left_on='key_applications',
                                         right_on='application_key',  how="inner")
        df_releases_to_update = df_releases_to_update[df_releases_to_update['id_epics'] !=
                                                      df_releases_to_update['old_release_id']]
        df_releases_to_update = df_releases_to_update[['application_id', 'application_key', 'feature_id', 'platform',
                                                       'id_epics', 'key_epics', 'segment', 'request_type']]
        df_releases_to_update = df_releases_to_update.sort_values(by='application_id')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_id', keep="first")
        df_releases_to_update.rename(columns={'id_epics':'release_id',  'key_epics': 'release_key'}, inplace=True)
        if df_releases_to_update.shape[0] > 0:
            self.logger.info(
                f'__UpdateReleaseDB__Info Перепривязали эпики у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
            # print(f'Перепривязали эпики у {df_releases_to_update.shape[0]} инициатив в RequestRelease')
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)
        df_releases_to_update = df_releases_to_update[
            ['application_id', 'application_key', 'release_id', 'release_key']]
        application_id_lst = list(set(df_releases_to_update['application_id'].to_list()))
        if len(application_id_lst) == 0:
            return #нечего обновлять
        df_rel_swell = self.get_db_data('relation_swell_feature',
                                       query_params={'application_id': application_id_lst})
        df_rel_swell.rename(columns={'release_key': 'release_key_epic'}, inplace=True)
        df_rel_swell_to_update = pd.merge(df_releases_to_update, df_rel_swell, on='application_id', how="inner")
        df_rel_swell_to_update = df_rel_swell_to_update[df_rel_swell_to_update['release_key']
                                                        != df_rel_swell_to_update['swell_key']]
        df_rel_swell_to_update.rename(columns={'release_key': 'swell_key', 'release_key_epic':'release_key' }, inplace=True)
        df_rel_swell_to_update = df_rel_swell_to_update[['application_id', 'platform',  'swell_key', 'release_key',
                                                         'request_type', 'result', 'error_code', 'last_update']]
        if df_rel_swell_to_update.shape[0]>0:
            self.logger.info(
                f'__UpdateReleaseDB__Info Перепривязали эпики у {df_rel_swell_to_update.shape[0]} инициатив в RelationSwellFeature')
            # print(f'Перепривязали эпики у {df_rel_swell_to_update.shape[0]} инициатив в RelationSwellFeature')
            self.insert_db_data(RelationSwellFeature, df_rel_swell_to_update)

    def update_application_keys(self, project_data, project_type):
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе
        key_releases_lst = list(set(project_data['key_releases'].to_list()))
        if project_type == 'mp_web':
            df_db_rel_feat = self.get_db_data('relation_release_feature',
                                                    query_params={'release_key': key_releases_lst})
            df_releases_to_update = pd.merge(project_data, df_db_rel_feat,
                                             left_on=['key_releases', 'fields.labels_applications', 'key_stories'],
                                             right_on=['release_key', 'platform', 'story_key'], how="inner")
            df_releases_to_update = df_releases_to_update[['application_id', 'key_applications']]

        elif project_type == 'ufs':
            project_data['key_stories']=''
            df_db_rel_feat = self.get_db_data('relation_swell_feature',
                                                    query_params={'swell_key': key_releases_lst})
            df_releases_to_update = pd.merge(project_data, df_db_rel_feat,
                                             left_on=['key_epics', 'fields.labels_applications'],
                                             right_on=['release_key', 'platform'], how="inner")
            df_releases_to_update = df_releases_to_update[['application_id', 'key_applications']]

        application_id_lst = list(set(df_releases_to_update['application_id'].to_list()))
        if len(application_id_lst) == 0:
            return #нечего обновлять
        # Получим данные которые есть по уже созданным application_id (для обновения по ним application_key)
        df_releases = self.get_db_data('requests_to_release', query_params={'application_id': application_id_lst})
        df_releases_to_update = pd.merge(df_releases, df_releases_to_update, on='application_id', how="inner")

        df_releases_to_update = df_releases_to_update.sort_values(by='application_id')
        df_releases_to_update = df_releases_to_update.drop_duplicates(subset='application_id', keep="first")
        # Оставим те строки где либо application_key не заполнен либо он не совпадает в jira (перепривязали/неверно привязали)
        df_releases_to_update = df_releases_to_update[(pd.isna(df_releases_to_update['application_key']) == True) | (
                    df_releases_to_update['application_key'] == "") | (df_releases_to_update['application_key'] !=
                                                                       df_releases_to_update['key_applications'])]
        df_releases_to_update['application_key'] = df_releases_to_update['key_applications']
        df_releases_to_update = df_releases_to_update[
            ['application_id', 'application_key', 'feature_id', 'platform', 'release_id', 'release_key', 'segment',
             'request_type']]
        if df_releases_to_update.shape[0]>0:
            self.insert_db_data(RequestRelease, df_releases_to_update, update=True)

    def update_RequestRelease(self, project_data, release_type):
        df_apps_to_create = project_data[pd.isna(project_data['application_id']) == True]
        df_apps_to_create = df_apps_to_create.sort_values(by='key_applications')
        df_apps_to_create = df_apps_to_create.drop_duplicates(subset='key_applications', keep="first")
        df_apps_to_create = df_apps_to_create[
            ['application_id', 'key_applications', 'feature_id', f'fields.labels_{release_type}', f'id_{release_type}',
             f'key_{release_type}']]
        df_apps_to_create.rename(
            columns={'key_applications': 'application_key', f'fields.labels_{release_type}': 'platform',
                     f'id_{release_type}': 'release_id', f'key_{release_type}': 'release_key'}, inplace=True)
        df_apps_to_create['segment'] = 'no-segment'
        df_apps_to_create['request_type'] = 'no-type'
        df_apps_to_create['application_id'] = df_apps_to_create['application_id'].apply(self.generate_id)
        self.insert_db_data(RequestRelease, df_apps_to_create)
        df_apps_to_create = df_apps_to_create[['application_id', 'application_key']]
        df_apps_to_create.rename(
            columns={'application_id': 'application_id_new', 'application_key': 'key_applications'}, inplace=True)
        project_data = pd.merge(project_data, df_apps_to_create, on='key_applications', how="left")
        project_data['application_id'] = np.where(pd.isna(project_data['application_id']) == True,
                                                  project_data['application_id_new'], project_data['application_id'])
        return project_data


    def update_RelationRelFeature(self, project_data):
        df_new_relat = pd.DataFrame()
        df_new_relat = project_data[
            ['application_id', 'fields.labels_applications', 'fields.summary_stories', 'key_stories', 'key_releases']]
        df_new_relat.rename(
            columns={'fields.labels_applications': 'platform', 'fields.summary_stories': 'feature_name', 'key_stories': 'story_key',
                     'key_releases': 'release_key'}, inplace=True)
        df_new_relat['type'] = ""
        df_new_relat['result'] = ""
        df_new_relat['error_code'] = ""
        df_new_relat['last_update'] = str(datetime.datetime.now().replace(microsecond=0))
        df_new_relat = df_new_relat.sort_values(by='application_id')
        df_new_relat = df_new_relat.drop_duplicates(subset='application_id', keep="first")
        self.insert_db_data(RelationRelFeature, df_new_relat)

    def update_DOR(self, project_data, project_type, DOR_type):  # subtasks, UXUI
        DOR_jira_key = 'key_' + DOR_type
        DOR_jira_lables = 'fields.labels_' + DOR_type
        DOR_jira_lables_1 = 'fields.labels_' + DOR_type + '_1'
        DOR_jira_status = 'fields.status.name_' + DOR_type
        DOR_content = 'content_' + DOR_type
        DOR_assignee_key = 'fields.assignee.key_' + DOR_type
        DOR_assignee_email = 'fields.assignee.emailAddress_' + DOR_type
        DOR_assignee_name = 'fields.assignee.displayName_' + DOR_type
        DOR_last_update = 'fields.updated_' + DOR_type
        DOR_description_wo_url = 'fields.description_wo_url_' + DOR_type
        DOR_urls = 'urls_' + DOR_type
        DOR_type_id = 'dor_type_id_'+ DOR_type

        df_dor_information = self.get_db_data('dor_info')
        project_data = pd.merge(project_data, df_dor_information, left_on= DOR_type_id, right_on='dor_type_id',
                                how="inner")
        project_data['content'] = ''
        project_data['content'] = np.where(project_data['dor_type'] == 'string' , project_data[DOR_description_wo_url],
                                           project_data['content'])
        project_data['content'] = np.where( project_data['dor_type'] == 'readyornot', project_data[DOR_description_wo_url],
                                           project_data['content'])
        project_data['content'] = np.where(project_data['dor_type'] == 'link', project_data[DOR_urls],
                                           project_data['content'])
        project_data['content'] = np.where(project_data['dor_type'] == 'jira-link', project_data[DOR_jira_key],
                                           project_data['content'])
        project_data[DOR_assignee_key] = np.where(pd.isna(project_data[DOR_assignee_key]) == True, '',
                                                  project_data[DOR_assignee_key])
        project_data[DOR_assignee_email] = np.where(pd.isna(project_data[DOR_assignee_email]) == True, '',
                                                    project_data[DOR_assignee_email])
        project_data[DOR_assignee_name] = np.where(pd.isna(project_data[DOR_assignee_name]) == True, '',
                                                   project_data[DOR_assignee_name])
        db_new_dor = pd.DataFrame()
        db_new_dor = project_data[
            ['application_id', DOR_jira_key, 'dor_type_id', 'content', DOR_jira_status, DOR_assignee_key,
             DOR_assignee_email, DOR_assignee_name, DOR_last_update]]
        db_new_dor.rename(
            columns={DOR_jira_key: 'subtask_key', DOR_jira_status: 'status', DOR_assignee_key: 'assignee_key',
                     DOR_assignee_email: 'assignee_email', DOR_assignee_name: 'assignee_name',
                     DOR_last_update: 'last_update'}, inplace=True)
        db_new_dor['user'] = ''
        db_new_dor = db_new_dor.sort_values(by=['application_id', 'subtask_key'])
        db_new_dor = db_new_dor.drop_duplicates(subset=['application_id', 'subtask_key'], keep="first")
        df_db_dor = self.get_db_data('dor')
        df_db_dor['exist_in_db'] = 'X'
        db_new_dor = pd.merge(db_new_dor, df_db_dor,
                        on=['application_id', 'subtask_key', 'dor_type_id', 'content', 'status', 'user', 'assignee_key',
                            'assignee_name', 'assignee_email', 'last_update'], how="left")
        db_new_dor = db_new_dor[db_new_dor['exist_in_db'] != 'X']
        db_new_dor = db_new_dor[['application_id', 'subtask_key', 'dor_type_id', 'content', 'status', 'user', 'assignee_key',
                     'assignee_name', 'assignee_email', 'last_update']]
        self.insert_db_data(DOR, db_new_dor, update=True)

    def update_RelationSwellFeature(self, project_data):
        df_new_swell = pd.DataFrame()
        df_new_swell = project_data[['application_id', 'fields.labels_epics', 'fields.labels_applications', 'key_epics', 'release_key']]
        df_new_swell.rename(columns={'fields.labels_applications': 'request_type', 'fields.labels_epics': 'platform',
                                     'key_epics': 'swell_key'}, inplace=True)
        df_new_swell['feature_name'] = ''
        df_new_swell['release_key'] = ''
        df_new_swell['result'] = ''
        df_new_swell['error_code'] = ''
        df_new_swell['last_update'] = ''
        df_new_swell = df_new_swell.sort_values(by='application_id')
        df_new_swell = df_new_swell.drop_duplicates(subset='application_id', keep="first")
        self.insert_db_data(RelationSwellFeature, df_new_swell)

    def update_mp_web_db(self, project_data):
        self.update_ReleaseRow(project_data, 'releases')
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе/ эпику и платорме
        self.update_application_keys(project_data, 'mp_web')
        self.update_release_mp_web(project_data)
        # Сгенерируем feature_id для тех key_releases-key_stories-platform, для которых их еще нет
        project_data = self.update_FeatureDescriptionRow(project_data, 'stories', 'releases')
        # Сгенерируем application_id для тех  application_key-feature_id-platform-release_id для которых их еще нет
        project_data = self.update_RequestRelease(project_data, 'releases')
        # Сохраним связь новых application_id (если они есть) со stories
        self.update_RelationRelFeature(project_data)
        self.update_DOR(project_data, 'mp_web', 'subtasks')
        self.update_DOR(project_data, 'mp_web', 'UXUI')

    def update_ufs_db(self, project_data):
        self.update_ReleaseRow(project_data, 'epics')
        # Для тех заявок в релиз, которые создавались через портал нужно проапдейтить/заполнить application_key по application_id
        # Для этого сначала найдем уже созданные application_id по релизу, платорме и сторе/ эпику и платорме
        self.update_application_keys(project_data, 'ufs')
        self.update_release_ufs(project_data)
        # Сгенерируем feature_id для тех key_epics-key_applications, для которых их еще нет
        project_data = self.update_FeatureDescriptionRow(project_data, 'applications', 'epics')
        project_data = self.update_RequestRelease(project_data, 'epics')
        self.update_RelationSwellFeature(project_data)
        self.update_DOR(project_data, 'ufs', 'subtasks')  # subtasks, UXUI
        self.update_DOR(project_data, 'ufs', 'UXUI')
