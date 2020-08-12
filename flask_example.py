from flask import Flask, send_from_directory, current_app
from flask import render_template, request
import os
import sys
from sshtunnel import SSHTunnelForwarder
import sshtunnel
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.orm import mapper, sessionmaker
import json
from flask import jsonify
import pymysql
from datetime import datetime, timedelta
import copy
import pandas as pd
import numpy as np
import uuid


from mysql_orm import FeatureDescriptionRow, RequestRelease, RelationRelFeature, EribRequest, LogTable, \
    RelationSwellFeature, ReleaseRow,  ProjectDescription,  DOR,  DORInformation,   ReleaseDates,  Base


def open_tunnel():
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
    tunnel.start()
    return tunnel


def create_db_session(local_bind_port, Base):
    db_engine_param =  'mysql://MariaChichkan:passw@127.0.0.1:%s/MariaChichkan$release_portal'
    engine = create_engine(db_engine_param % local_bind_port, pool_recycle=3600, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session(), engine


def check_connection(tunnel):
    if not tunnel.is_alive:
        tunnel.restart()


pymysql.install_as_MySQLdb()
app = Flask(__name__, static_url_path='')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


@app.route('/portal/api/get-feature/<path:feature_id>')
def feature_card(feature_id):
    global session
    global tunnel
    try:
        check_connection(tunnel)
        df_feature_info = pd.read_sql(
            session.query(FeatureDescriptionRow).filter_by(feature_id=feature_id).limit(1).statement, engine)
        feature_info = {"title": df_feature_info['name'][0],
        "user": "Rashitov-MT",   #df_feature_info['user'][0],
        "created": df_feature_info['created'][0],
        "updated": df_feature_info['updated'][0]}
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from FeatureDescriptionRow fail:{str(ex)}'})

    try:
        # подянем релизы-application по ID фичи
        check_connection(tunnel)
        df_req_release = pd.read_sql(
            session.query(RequestRelease).filter_by(feature_id=feature_id).statement, engine)
        df_req_release = df_req_release[
            ['application_id', 'application_key', 'feature_id', 'release_id', 'release_key', 'segment', 'request_type']]
        application_lst = list(set(df_req_release['application_id'].to_list()))
        application_key_lst = list(set(df_req_release['application_key'].to_list()))
        release_lst = list(set(df_req_release['release_id'].to_list()))
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from RequestRelease fail:{str(ex)}'})
    try:
        project_key = application_key_lst[0].split('-')[0]
        df_project_description = pd.read_sql(
           session.query(ProjectDescription).filter_by(key=project_key).statement, engine)
        if df_project_description.shape[0] > 0:
            project_name = df_project_description['name'][0]
        else:
            project_name =""
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from ProjectDescription fail:{str(ex)}'})

    try:
        # Подтянем информацию по релизу
        check_connection(tunnel)
        df_release_row = pd.read_sql(
           session.query(ReleaseRow).filter(ReleaseRow.release_id.in_(release_lst)).statement, engine)
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from ReleaseRow fail:{str(ex)}'})
    try:
        df_release_dates = pd.read_sql(
            session.query(ReleaseDates).filter(ReleaseDates.release_id.in_(release_lst)).statement, engine)
    except Exception as ex:
        return jsonify({'body': f'db select from ReleaseDates fail:{str(ex)}'})
    try:
        # Выберем stories по каждому из application
        check_connection(tunnel)
        df_rel_feature = pd.read_sql(session.query(RelationRelFeature).filter(
            RelationRelFeature.application_id.in_(application_lst)).statement, engine)
       #df_rel_feature['application_id'] = df_rel_feature['application_id'].astype(int)
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from RelationRelFeature fail:{str(ex)}'})

    df_req_release = pd.merge(df_req_release, df_release_row, on='release_id', how="inner")
    df_req_release.rename(columns={'platform': 'platform_release'}, inplace=True)
    df_releases_stories_apps = pd.merge(df_req_release, df_rel_feature, on=['application_id', 'release_key'],
                                        how="inner")

    try:
        check_connection(tunnel)
        stories_lst = df_releases_stories_apps['story_key'].to_list()
        if len(stories_lst)>0:
            story_key = stories_lst[0].split('-')[0]
            df_project_description = pd.read_sql(
               session.query(ProjectDescription).filter_by(key=story_key).statement, engine)
            if df_project_description.shape[0] > 0:
                team_name = df_project_description['name'][0]
            else:
                team_name =""
        else:
            team_name = ""
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from ProjectDescription fail:{str(ex)}'})

    # Выберем записи свеллов по каждому из application
    try:
        check_connection(tunnel)
        df_swell_feature = pd.read_sql(session.query(RelationSwellFeature).filter(
            RelationSwellFeature.application_id.in_(application_lst)).statement, engine)
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from RelationSwellFeature fail:{str(ex)}'})

    df_swell_feature = df_swell_feature[['application_id', 'platform', 'swell_key', 'request_type']]
    df_swell_feature.rename(columns={'request_type': 'request_type_swell'}, inplace=True)
    df_swell_feature = pd.merge(df_req_release, df_swell_feature, left_on=['application_id', 'release_key'],
                                right_on=['application_id', 'swell_key'], how="inner")

    try:
        # Подтянем все ДОРы по каждому из application
        check_connection(tunnel)
        df_dor = pd.read_sql(session.query(DOR).filter(DOR.application_id.in_(application_lst)).statement, engine)
        df_dor['DOR-stat'] = 1
        df_statuses = df_dor.groupby(['application_id', 'status'])['DOR-stat'].sum().reset_index()
        df_dor = df_dor[["application_id", "subtask_key", "dor_type_id", "content", "status", "user", "assignee_key",
                         "assignee_name", "assignee_email", "last_update"]]
        df_dor.rename(columns={"application_id": "application-id","subtask_key":"subtask-key", "dor_type_id": "dor-type-id",
                               "assignee_key": "assignee-key", "assignee_name": "assignee-name",
                               "assignee_email":"assignee-email", "last_update": "last-update"}, inplace=True)
        dor_type_id_lst = list(set(df_dor["dor-type-id"].to_list()))
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from DOR fail:{str(ex)}'})
    try:
        # Подтянем инфомацию по тем типам ДОРов которые есть в выборке ДОРов
        check_connection(tunnel)
        df_dor_info = pd.read_sql(
            session.query(DORInformation).filter(DORInformation.dor_type_id.in_(dor_type_id_lst)).statement, engine)
        df_dor_info = df_dor_info[
            ["dor_type_id", "category", "dor_name", "label", "short_description", "upper_text", "placeholder",
             "description", "dor_type", "close_date"]]
        df_dor_info = df_dor_info.sort_values(by='dor_type_id')
        df_dor_info = df_dor_info.drop_duplicates(subset='dor_type_id', keep="first")
        df_dor_info.rename(columns={"dor_type_id": "dor-type-id", "dor_name": "name", "short_description": "short-desc",
                                    "upper_text": "upper-text", "dor_type": "type", "close_date": "close-day",
                                    }, inplace=True)
    except Exception as ex:
        print(ex)
        return jsonify({'body': f'db select from DORInformation fail:{str(ex)}'})
    df_statuses = df_statuses[df_statuses['status'].isin(['to-fill', 'to-fix', 'in-progress', 'complete']) == True]
    df_statuses['status'] = np.where(df_statuses['status'] == 'to-fill', 'DOR-to-fill', df_statuses['status'])
    df_statuses['status'] = np.where(df_statuses['status'] == 'to-fix', 'DOR-to-fix', df_statuses['status'])
    df_statuses['status'] = np.where(df_statuses['status'] == 'in-progress', 'DOR-in-progress', df_statuses['status'])
    df_statuses['status'] = np.where(df_statuses['status'] == 'complete', 'DOR-complete', df_statuses['status'])

    release_links = []

    df_all_statuses = df_statuses[['status', 'DOR-stat']].set_index('status')
    DOR_stat = df_all_statuses.to_dict()

    for index, release_row in df_releases_stories_apps.iterrows():
        df_app_statuses = df_statuses[df_statuses['application_id'] == release_row['application_id']]
        df_app_statuses = df_app_statuses[['status', 'DOR-stat']].set_index('status')
        DOR_app_stat = df_app_statuses.to_dict()

        release_links.append({"platform": release_row.platform_release,
                              "release-id": release_row.release_id,
                              "story-keys": release_row.story_key,
                              "type": release_row.type,
                              "application-id": release_row.application_id,
                              "application-key": release_row.application_key,
                              "application-status": "",
                              "DOR-stat": DOR_app_stat['DOR-stat'],
                              })

    for index, release_row in df_swell_feature.iterrows():
        df_app_statuses = df_statuses[df_statuses['application_id'] == release_row['application_id']]
        df_app_statuses = df_app_statuses[['status', 'DOR-stat']].set_index('status')
        DOR_app_stat = df_app_statuses.to_dict()

        release_links.append({"platform": release_row.platform_release,
                              "swell-id": release_row.release_id,
                              "type": release_row.request_type_swell,
                              "channels": "",
                              "release-keys": "",
                              "application-id": release_row.application_id,
                              "application-key": release_row.application_key,
                              "application-status": "",
                              "DOR-stat": DOR_app_stat['DOR-stat'],
                              })

    dor_info = df_dor_info.to_dict('records')
    dor = df_dor.to_dict('records')

    releases = []
    for ind, row in df_release_row.iterrows():
        dates = {}
        df_dates = df_release_dates[df_release_dates['release_id'] == row['release_id']]
        if df_dates.shape[0] > 0:
            for ind, daterow in df_dates.iterrows():
                dates[daterow['date_type']] = daterow['date']
        release = {'release-id': row['release_id'],
                   'name': row['name'],
                   'platform': row['platform'],
                   'key': row['jira_key'],
                   'dates': dates}
        releases.append(release)
    if team_name == "":
        team_name = project_name

    body = {
        "title": feature_info["title"],
        "user": "Rashitov-MT",
        "team": team_name,
        "created": feature_info["created"],
        "updated": feature_info["updated"],
        "DOR-stat": DOR_stat['DOR-stat'],
        'releases': releases,
        "release-links" :release_links,
        "DOR-info": dor_info,
        "DOR": dor}
    feature_json = {'body': [body]}
    return jsonify(feature_json)


@app.route('/portal/api/get-card-release/<path:release_id>')
def release_card(release_id):
    try:
        # Подтянем инфомацию по релизу
        check_connection(tunnel)
        df_release_info = pd.read_sql(
            session.query(ReleaseRow).filter_by(release_id=release_id).statement, engine)
        if df_release_info.shape[0] == 0:
            return jsonify({'body': f"release_id {release_id} doesn't exist"})

        # Подтянем инфомацию по релизу
        check_connection(tunnel)
        df_release_dates = pd.read_sql(
            session.query(ReleaseDates).filter_by(release_id=release_id).statement, engine)

        # Подтянем все фичи по релизу
        check_connection(tunnel)
        df_requests_release = pd.read_sql(
            session.query(RequestRelease).filter_by(release_id=release_id).statement, engine)

        application_lst = list(set(df_requests_release['application_id'].to_list()))
        feature_lst = list(set(df_requests_release['feature_id'].to_list()))

        # Подтянем
        check_connection(tunnel)
        df_features_descr = pd.read_sql(
            session.query(FeatureDescriptionRow).filter(FeatureDescriptionRow.feature_id.in_(feature_lst)).statement,
            engine)

        # Подтянем
        check_connection(tunnel)
        df_relation_rel = pd.read_sql(
            session.query(RelationRelFeature).filter(RelationRelFeature.application_id.in_(application_lst)).statement,
            engine)

        # Подтянем
        check_connection(tunnel)
        df_dor = pd.read_sql(
            session.query(DOR).filter(DOR.application_id.in_(application_lst)).statement, engine)

        dor_types_lst = list(set(df_dor['dor_type_id'].to_list()))

        # Подтянем
        check_connection(tunnel)
        df_dor_info = pd.read_sql(
            session.query(DORInformation).filter(DORInformation.dor_type_id.in_(dor_types_lst)).statement, engine)

        check_connection(tunnel)
        teams = session.query(ProjectDescription).all()
        team_dict = {row.key: row.name for row in teams}

        release_dates = {}
        for ind, row in df_release_dates.iterrows():
            release_dates[row['date_type']] = row['date']

        dor_info = {}
        print(df_dor_info.columns)
        for ind, row in df_dor_info.iterrows():
            dor_info[row['dor_type_id']] = {
                "category": row['category'],
                "name": row['dor_name'],
                "short-name": row["short_name"],
                "label": row['label'],
                "short-desc": row['short_description'],
                "upper-text": row['upper_text'],
                "placeholder": row['placeholder'],
                "description": row['description'],
                "type": row['dor_type'],
                "close-day": row['close_date'],
                "show-in-details": row["show_in_details"]
            }

        features = []
        for feature in feature_lst:
            df_feature_descr = df_features_descr[df_features_descr['feature_id'] == feature].reset_index()
            feature_applications = df_requests_release[df_requests_release['feature_id'] == feature][
                'application_id'].to_list()
            feature_stories = df_relation_rel[df_relation_rel['application_id'].isin(feature_applications)][
                'story_key'].to_list()
            df_feature_dors = df_dor[df_dor['application_id'].isin(feature_applications)]
            team_key = ""
            team_name = ""
            if len(feature_stories) > 0:
                team_key = feature_stories[0].split('-')[0]
            if team_key in team_dict:
                team_name = team_dict[team_key]

            feature_dors = {}
            for ind, row in df_feature_dors.iterrows():
                feature_dors[row['dor_type_id']] = {"subtask-key": row['subtask_key'],
                                                    "content": row["content"],
                                                    "status": row['status']}
            features.append(
                {"title": df_feature_descr['name'][0],
                 "feature-id": feature,
                 "user": df_feature_descr['user'][0],
                 "team": team_name,
                 "created": df_feature_descr['created'][0],
                 "updated": df_feature_descr['updated'][0],
                 "story-keys": feature_stories,
                 "status-name": "",
                 "DOR": feature_dors})
        release_info = {'body': [{
            "platform": df_release_info['platform'][0],
            "name": df_release_info['name'][0],
            "id": df_release_info['release_id'][0],
            "key": df_release_info['jira_key'][0],
            "page": "",
            "status": "",
            "stat": {
                "features-psi-ready": "10",
                "features-psi-notallowed": "67",
                "features-psi-passed": "0",
                "bugs": "464"
            },
            "dates": release_dates,
            "DOR-info": dor_info,
            "features": features}]}

        return jsonify(release_info)
    except Exception as ex:
        return jsonify({'body': f'{str(ex)}'})


tunnel = open_tunnel()
session, engine = create_db_session(tunnel.local_bind_port, Base)
# app.run()