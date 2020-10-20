from flask import Flask, send_from_directory, current_app
from flask import render_template, request
from sqlalchemy import or_, and_
import json
from flask import jsonify
import pymysql
from datetime import datetime, timedelta
import copy
import pandas as pd
import numpy as np
import uuid
import re
import sys
import os
from sqlalchemy.dialects.mysql import insert
from mysql_orm import FeatureDescriptionRow, RequestRelease, RelationRelFeature, EribRequest, LogTable, \
    RelationSwellFeature, ReleaseRow,  ProjectDescription,  DOR,  DORInformation,   ReleaseDates,  Base, MassFeature,\
    MassFeatureLink, ReleasePages
from utils import connect_jira, create_db_session, count_teams_and_features, TextParser

from sqlalchemy.pool import NullPool
from jira_extractor import  JiraDataByProject
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.orm import mapper, sessionmaker


pymysql.install_as_MySQLdb()
app = Flask(__name__, static_url_path='')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


@app.route('/api/update_json', methods=['POST'])
@app.route('/portal/api/update_json', methods=['POST'])
def update_json():
    content = request.get_json(silent=True)
    print(content)
    with open("release-dashboard.json", 'r+') as f:
        f.seek(0)
        json.dump(content, f, ensure_ascii=False)
        f.truncate()
    resp = jsonify(success=True)
    return resp



@app.route('/api/get-releases',  methods=['POST'])
@app.route('/portal/api/get-releases',  methods=['POST'])
def get_releases():
    global Session
    content = request.get_json(silent=True)
    body = {}
    mode = ""
    release_lst = []
    platform_lst = []
    date_from_str = ""
    date_to_str = ""

    if 'body' in content:
        body = content['body'][0]
    if 'mode' in body:
        mode = body['mode']
    if 'release-id' in body:
        release_lst = body['release-id']
    if 'date-from' in body:
        date_from_str = body['date-from']
    if 'date-to' in body:
        date_to_str = body['date-to']
    if  "platform" in body:
        platform_lst = body["platform"]
    if  mode == 'register-application' or (mode == "" and len(release_lst) == 0 and len(platform_lst) == 0 and
                                            not date_from_str and not date_to_str):
        mode = 'register-application'
    else:
        mode = 'full'

    session = Session()
    try:
        df_release = pd.read_sql(session.query(ReleaseRow).statement, engine)
        df_all_releasedates = pd.read_sql(session.query(ReleaseDates).statement, engine)
        df_release_pages = pd.read_sql(session.query(ReleasePages).statement, engine)
        df_all_releasedates['date_type'] = df_all_releasedates['date_type'].str.replace('_', '-')
        df_all_releasedates['date'] = df_all_releasedates['date'].str[0:11]
        df_all_releasedates['date'] = pd.to_datetime(df_all_releasedates["date"], format='%Y-%m-%d')
        df_release_with_dates = pd.merge(df_release, df_all_releasedates, on=['release_id', 'platform'], how="inner")
        df_dates_right_form = df_all_releasedates
        df_dates_right_form["date"] = df_dates_right_form["date"].dt.strftime('%Y-%m-%d')
    except Exception as ex:
        return jsonify({'body': f'{str(ex)}'})
    finally:
        session.close()
    if mode == 'register-application':
        df_release_with_dates = df_release_with_dates[(df_release_with_dates['date_type'] == 'freeze') | (
                    df_release_with_dates['date_type'] == 'publication-start')]
        N = 0
        if N > 0:
            df_release_with_dates = df_release_with_dates.sort_values(by=['platform', 'date'], ascending=[True, False])
            df_N_last_releases = pd.concat(
                [df_release_with_dates[df_release_with_dates['platform'] == key][0:N] for key in
                 set(df_release_with_dates['platform'].to_list())], axis=0)
            df_release_with_dates = df_N_last_releases
        today = pd.to_datetime('today')
        actual_release_lst = df_release_with_dates[((df_release_with_dates['date_type'] == 'freeze') | (df_release_with_dates['date_type'] == 'publication-start')) &
                                                   (df_release_with_dates['date'] > today)]['release_id'].to_list()
        df_release_with_dates = df_release_with_dates[df_release_with_dates['release_id'].isin(actual_release_lst)]
    elif mode == 'full':
        df_release_with_dates = df_release_with_dates[(df_release_with_dates['date_type'] == 'publication') | (
                    df_release_with_dates['date_type'] == 'publication-finish')]
        if len(release_lst) > 0:
            df_release_with_dates = df_release_with_dates[df_release_with_dates['release_id'].isin(release_lst)]
        else:
            if len(platform_lst) > 0:
                df_release_with_dates = df_release_with_dates[df_release_with_dates['platform'].isin(platform_lst)]
            if date_from_str:
                date_from = datetime.strptime(date_from_str, '%Y-%m-%d')
                df_release_with_dates = df_release_with_dates[df_release_with_dates['date'] > date_from]
            if date_to_str:
                date_to = datetime.strptime(date_to_str, '%Y-%m-%d')
                df_release_with_dates = df_release_with_dates[df_release_with_dates['date'] < date_to]

    df_release_with_dates = df_release_with_dates.sort_values(by=['platform', 'date'])
    df_release_pages = df_release_pages[
        df_release_pages['release_id'].isin(df_release_with_dates['release_id'].to_list())]
    releases = []
    for ind, row in df_release_with_dates.iterrows():
        dates_json = {}
        df_dates_release = df_dates_right_form[df_dates_right_form['release_id'] == row['release_id']]
        if mode == 'register-application':
            for ind, daterow in df_dates_release.iterrows():
                if daterow['date_type'] in ['freeze', 'publication', 'publication-start', 'publication-finish']:
                    dates_json[daterow['date_type']] = daterow['date']
        elif mode == 'full':
            for ind, daterow in df_dates_release.iterrows():
                dates_json[daterow['date_type']] = daterow['date']

        releases.append({"platform": row['platform'],
                         "name": row['name'],
                         "id": row['release_id'],
                         "key": row['jira_key'],
                         "dates": dates_json})

    release_pages = []
    for ind, row in df_release_pages.iterrows():
        release_pages.append({"release-id": row["release_id"], "page": row["page"]})

    release_json = {"body":
                        [{"user": "Rashitov-MT",
                          "releases": releases,
                          "web-page-prefix": "https://sbtatlas.sigma.sbrf.ru/wiki/pages/viewpage.action?pageId=",
                          "web-pages": release_pages}]}
    return jsonify(release_json)


@app.route('/portal/api/find-feature', methods=['POST'])
def find_feature_V2():
    global Session
    #global tunnel
    session = Session()

    release_id = None
    title = None
    datefrom = None
    dateto = None
    stories = []
    body = {}

    content = request.get_json(silent=True)
    if 'body' in content:
        body = content['body'][0]
    if 'release-id' in body:
        release_id = body['release-id']
    if 'title' in body:
        title = body['title']
    if 'date-from' in body:
        datefrom = body['date-from']
    if 'date-to' in body:
        dateto = body['date-to']
    if 'stories' in body:
        stories = body['stories']

    release_query = {}
    filters = []
    filters_ufs = []
    title_query = []

    if release_id:
        release_query = {'release_id': release_id}

    story_lst = list(filter(lambda x: '-' in x, stories))
    team_lst = list(filter(lambda x: '-' not in x, stories))
    team_lst = [f'{team}%' for team in team_lst]

    if len(story_lst) > 0:
        filters.append(RelationRelFeature.story_key.in_(story_lst))
        filters_ufs.append(RequestRelease.application_key.in_(story_lst))
    if len(team_lst) > 0:
        filters.append(or_(*[RelationRelFeature.story_key.like(team) for team in team_lst]))
        filters_ufs.append(or_(*[RequestRelease.application_key.like(team) for team in team_lst]))
    if title:
        title_query.append(FeatureDescriptionRow.name.like(f'%{title}%'))

    try:
        df_features = pd.read_sql(
            session.query(ReleaseRow.name.label('release_name'), RequestRelease, RelationRelFeature.story_key,
                          FeatureDescriptionRow.name, FeatureDescriptionRow.created,
                          FeatureDescriptionRow.updated).filter_by(**release_query).filter(or_(*filters)).filter(
                *title_query).join(ReleaseRow, ReleaseRow.release_id == RequestRelease.release_id).join(RelationRelFeature,
                                                                                                        RequestRelease.application_id == RelationRelFeature.application_id).join(
                FeatureDescriptionRow, RequestRelease.feature_id == FeatureDescriptionRow.feature_id).statement, engine)

        df_features_ufs = pd.read_sql(
            session.query(ReleaseRow.name.label('release_name'), RequestRelease,
                          RelationSwellFeature.request_type.label('type'), FeatureDescriptionRow.name,
                          FeatureDescriptionRow.created, FeatureDescriptionRow.updated).filter_by(**release_query).filter(
                or_(*filters_ufs)).filter(*title_query).join(ReleaseRow,
                                                             ReleaseRow.release_id == RequestRelease.release_id).join(
                RelationSwellFeature, RequestRelease.application_id == RelationSwellFeature.application_id).join(
                FeatureDescriptionRow, RequestRelease.feature_id == FeatureDescriptionRow.feature_id).statement, engine)

        df_features.rename(columns={'request_type': 'type'}, inplace=True)
        df_features_ufs['story_key'] = df_features_ufs['application_key']
        df_features_ufs = df_features_ufs[
            ['release_name', 'application_id', 'application_key', 'feature_id', 'platform', 'release_id', 'release_key',
             'segment', 'type', 'story_key', 'name', 'created', 'updated']]
        df_features = pd.concat([df_features, df_features_ufs], axis=0, ignore_index=True)

        df_features['created_date'] = pd.to_datetime(df_features['created'], format='%Y-%m-%d')
        if datefrom:
            datefrom = pd.to_datetime(datefrom)
            df_features = df_features[df_features['created_date'] >= datefrom]
        if dateto:
            dateto = pd.to_datetime(dateto)
            df_features = df_features[df_features['created_date'] <= dateto]

        df_features['team_key'] = df_features['story_key'].str.split('-').str[0]
        teams_lst = list(set(df_features['team_key'].to_list()))
        df_project_description = pd.read_sql(
            session.query(ProjectDescription).filter(ProjectDescription.key.in_(teams_lst)).statement, engine)

    except Exception as ex:
        return jsonify({{'body': ex}})
    finally:
        session.close()

    df_project_description.rename(columns={'key': 'team_key', 'name': 'team_name'}, inplace=True)
    df_features = pd.merge(df_features, df_project_description, on='team_key', how="left")
    df_features['team_name'].fillna("", inplace=True)
    if df_features.shape[0] == 0:
        return jsonify({"body": []})
    df_feature_stories = df_features.groupby(['feature_id', 'release_id'])['story_key'].apply(list).reset_index()
    df_feature_stories.rename(columns={'story_key': 'story_key_lst'}, inplace=True)
    df_features = df_features.sort_values(by=['feature_id', 'release_id'])
    df_features = df_features.drop_duplicates(subset=['feature_id', 'release_id'], keep="first")
    df_features = pd.merge(df_features, df_feature_stories, on=['feature_id', 'release_id'], how="left")
    feature_lst = list(set(df_features['feature_id'].to_list()))

    feature_json = []
    for feature in feature_lst:
        release_links = []
        df_feature = df_features[df_features['feature_id'] == feature]
        for ind, row in df_feature.iterrows():
            release_links.append({
                "platform": row['platform'],
                "release-id": row['release_id'],
                "release-name": row['release_name'],
                "story-keys": row['story_key_lst'],
                "type": row['type'],
                "application-id": row['application_id'],
                "application-key": row['application_key'],
                "application-status": ""
            })
        feature_json.append({"title": df_feature['name'].iloc[0],
                             "feature-id": df_feature['feature_id'].iloc[0],
                             "user": "Rashitov-MT",
                             "team": df_feature['team_name'].iloc[0],
                             "created": df_feature['created'].iloc[0],
                             "updated": df_feature['updated'].iloc[0],
                             "release-links": release_links})
    return jsonify({"body": feature_json})

@app.route('/portal/api/find_feature', methods=['POST'])
def find_feature():
    global Session
    global engine

    content = request.get_json(silent=True)
    print(content)

    session = Session()

    try:

        releas_db = session.query(ReleaseRow).all()
        releas_dict = {row.release_id: row for row in releas_db}

        teams = session.query(ProjectDescription).all()
        team_dict = {row.key: row.name for row in teams}

        feature_row = session.query(FeatureDescriptionRow).all()
        req_to_rel_row = session.query(RequestRelease).all()
    finally:
        session.close()

    dict_to_return = dict()
    dict_to_return['body'] = list()


    def find_team(key):
        project_key = key
        project_key = project_key[:project_key.find('-')]

        if project_key in team_dict:
            team_name = team_dict[project_key]
        else:
            team_name = "No_found"
        return team_name

    for feature in feature_row:
        dict_in_body = {
                        "title": feature.name,
                        "feature-id": feature.feature_id,
                        "user": "some guy",
                        "team": None,
                        "created": feature.created,
                        "updated": feature.updated,
                        "release-links": list()
                        }
        feature_id = dict_in_body["feature-id"]
        # req_to_rel_row = session.query(RequestRelease).filter(RequestRelease.feature_id == feature_id)
        req_to_rel_row_new = [row for row in req_to_rel_row if row.feature_id == feature_id]
        if len(req_to_rel_row_new) == 0:
            continue
        for rtrr in req_to_rel_row_new:
            release_dict = {
                            "platform": rtrr.platform,
                            "release-id": rtrr.release_id,
                            "release-name": "",
                            # "release-name": releas_dict[rtrr.release_id].name,
                            "story-keys": [rtrr.application_key],
                            "type": None,
                            "application-id": rtrr.application_id,
                            "application-key": rtrr.application_key,
                            "application-status": None
                            }
            dict_in_body["release-links"].append(release_dict)

        dict_in_body["team"] = find_team(release_dict["application-key"])

        dict_to_return['body'].append(dict_in_body)


@app.route('/api/get_dashboard')
@app.route('/portal/api/get_dashboard')
def get_json():
    return send_from_directory(current_app.root_path, "release-dashboard.json")

@app.route('/portal/api/get-feature/<path:feature_id>')
def feature_card(feature_id):
    global Session
    session = Session()
    try:
        try:
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
        if len(application_key_lst) == 0:
            return jsonify({'body': []})

        try:
            project_key = application_key_lst[0].split('-')[0]
            df_project_description = pd.read_sql(
               session.query(ProjectDescription).filter_by(key=project_key).statement, engine)
            if df_project_description.shape[0] > 0:
                project_name = df_project_description['name'].iloc[0]
            else:
                project_name =""
        except Exception as ex:
            print(ex)
            return jsonify({'body': f'db select from ProjectDescription fail:{str(ex)}'})

        try:
            # Подтянем информацию по релизу
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
            stories_lst = df_releases_stories_apps['story_key'].to_list()
            if len(stories_lst)>0:
                story_key = stories_lst[0].split('-')[0]
                df_project_description = pd.read_sql(
                   session.query(ProjectDescription).filter_by(key=story_key).statement, engine)
                if df_project_description.shape[0] > 0:
                    team_name = df_project_description['name'].iloc[0]
                else:
                    team_name =""
            else:
                team_name = ""
        except Exception as ex:
            print(ex)
            return jsonify({'body': f'db select from ProjectDescription fail:{str(ex)}'})

        # Выберем записи свеллов по каждому из application
        try:
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
            # Подтянем все активные ДОРы по каждому из application
            df_dor_wth_info = pd.read_sql(
                session.query(DOR.application_id, DOR.subtask_key, DOR.content, DOR.status, DOR.user, DOR.assignee_key,
                              DOR.assignee_name, DOR.assignee_email, DOR.last_update, DORInformation).join(
                    DORInformation, DOR.dor_type_id == DORInformation.dor_type_id).filter(
                    and_(DORInformation.active == 'true', DOR.application_id.in_(application_lst))).statement, engine)
            df_dor_wth_info['DOR-stat'] = 1
            df_statuses = df_dor_wth_info.groupby(['application_id', 'status'])['DOR-stat'].sum().reset_index()
            df_dor = df_dor_wth_info[
                ["application_id", "subtask_key", "dor_type_id", "content", "status", "user", "assignee_key",
                 "assignee_name", "assignee_email", "last_update"]]
            df_dor.rename(
                columns={"application_id": "application-id", "subtask_key": "subtask-key", "dor_type_id": "dor-type-id",
                         "assignee_key": "assignee-key", "assignee_name": "assignee-name",
                         "assignee_email": "assignee-email", "last_update": "last-update"}, inplace=True)
            df_dor_info = df_dor_wth_info[
                ["dor_type_id", "category", "dor_name", "label", "short_description", "upper_text", "placeholder",
                 "description", "dor_type", "close_date"]]
            df_dor_info = df_dor_info.sort_values(by='dor_type_id')
            df_dor_info = df_dor_info.drop_duplicates(subset='dor_type_id', keep="first")
            df_dor_info.rename(
                columns={"dor_type_id": "dor-type-id", "dor_name": "name", "short_description": "short-desc",
                         "upper_text": "upper-text", "dor_type": "type", "close_date": "close-day",
                         }, inplace=True)
        except Exception as ex:
            print(ex)
            return jsonify({'body': f'db select from DOR or DORInformation fail:{str(ex)}'})

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
            if row['platform'] == 'ufs':
                release = {'swell-id': row['release_id'],
                           'name': row['name'],
                           'platform': row['platform'],
                           'key': row['jira_key'],
                           'dates': dates}
            else:
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
    finally:
        session.close()
    return jsonify(feature_json)


@app.route('/portal/api/get-card-release/<path:release_id>')
def release_card(release_id):
    global Session
    session = Session()
    try:
        # Подтянем инфомацию по релизу
        df_release_info = pd.read_sql(
            session.query(ReleaseRow).filter_by(release_id=release_id).statement, engine)
        if df_release_info.shape[0] == 0:
            return jsonify({'body': f"release_id {release_id} doesn't exist"})

        df_release_dates = pd.read_sql(
            session.query(ReleaseDates).filter_by(release_id=release_id).statement, engine)

        df_requests_release = pd.read_sql(
            session.query(RequestRelease).filter_by(release_id=release_id).statement, engine)

        application_lst = list(set(df_requests_release['application_id'].to_list()))
        feature_lst = list(set(df_requests_release['feature_id'].to_list()))

        df_features_descr = pd.read_sql(
            session.query(FeatureDescriptionRow).filter(FeatureDescriptionRow.feature_id.in_(feature_lst)).statement,
            engine)

        df_relation_rel = pd.read_sql(
            session.query(RelationRelFeature).filter(RelationRelFeature.application_id.in_(application_lst)).statement,
            engine)

        df_dor = pd.read_sql(
            session.query(DOR).join(DORInformation, DOR.dor_type_id == DORInformation.dor_type_id).filter(
                and_(DORInformation.active == 'true', DOR.application_id.in_(application_lst))).statement, engine)

        dor_types_lst = list(set(df_dor['dor_type_id'].to_list()))

        df_dor_info = pd.read_sql(
            session.query(DORInformation).filter(DORInformation.dor_type_id.in_(dor_types_lst)).statement, engine)

        teams = session.query(ProjectDescription).all()
        team_dict = {row.key: row.name for row in teams}

        release_dates = {}
        for ind, row in df_release_dates.iterrows():
            release_dates[row['date_type']] = row['date']

        dor_info = {}
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
                "teams": "53",
                "features": "77",
                "features-psi-ready": "10",
                "features-psi-notallowed": "67",
                "features-psi-passed": "0",
                "bugs": "464"
            },
            "dates": release_dates,
            "DOR-info": dor_info,
            "features": features}]}
        session.close()
        return jsonify(release_info)
    except Exception as ex:
        session.close()
        return jsonify({'body': f'{str(ex)}'})

@app.route('/portal/api/update-DOR', methods=['POST'])
def update_dor():
    global Session
    session = Session()
    content = request.get_json(silent=True)
    print(content)
    try:
        application_id = content['body'][0]['application-id']
        dor_type_id = content['body'][0]['dor-type-id']
        new_content = content['body'][0]['content']
        dor = session.query(DOR).filter_by(application_id=application_id, dor_type_id=dor_type_id).one()
        dor_info = session.query(DORInformation).filter_by(dor_type_id=dor_type_id).one()
        subtask_key = dor.subtask_key
        print(f'subtask_key:{subtask_key}')
        # Get an issue.
        jira = connect_jira()
        issue = jira.issue(subtask_key)
    except Exception as ex:
        session.close()
        return {'body':[{
            "result": "",
            "result-text": f"[update-DOR] - error - Заявка в релиз {application_id}. ДОР - {dor_type_id}. Содержание - {new_content} .Ошибка - {str(ex)}"
        }]}

    old_content = TextParser.normalize_text(issue.fields.description)
    old_content = TextParser.remove_tables(old_content)
    old_content = old_content.lstrip()
    old_content = old_content.rstrip()

    if dor_info.dor_type == 'string' or dor_info.dor_type == 'link' or dor_info.dor_type == 'jira-link':
        new_jira_status = 'In Progress'
        new_db_status = 'in-progress'

    elif dor_info.dor_type == 'readyornot':
        if new_content:
            new_content = "Готово"
            new_jira_status = 'Done'
            new_db_status = 'complete'

        elif not new_content:
            new_content = ""
            new_jira_status = 'To Do'
            new_db_status = 'to-do'
        else:
            return {'body': [{"result": "",
                              "result-text": f"[update-DOR] - error - Заявка в релиз {application_id}. ДОР - {dor_type_id}. Содержание - {new_content} .Ошибка - С этим типом ДОР контент должен быть True или False"
                          }]}
    else:
        return {'body': [{"result": "",
                          "result-text": f"[update-DOR] - error - Заявка в релиз {application_id}. ДОР - {dor_type_id}. Содержание - {new_content} .Ошибка - С этим типом ДОР редактирование невозможно"
                          }]}
    if len(old_content) > 0:
        new_description = re.sub(old_content, new_content, issue.fields.description)
    elif len(old_content) == 0 and len(new_content) > 0:
        new_description = issue.fields.description + '\r\n' + new_content + '\r\n'
        regex = re.compile(r'[\r\n]+')
        new_description = regex.sub(r'\r\n', new_description)
    else:
        new_description = issue.fields.description

    update_datetme = str(datetime.now().replace(microsecond=0))
    try:
        # Update status
        jira.transition_issue(issue, transition=new_jira_status)
        # update description
        issue.update(fields={"description": new_description})
        session.query(DOR).filter_by(application_id=application_id, dor_type_id=dor_type_id).update(
            {'status': new_db_status, 'content': new_content, 'last_update': update_datetme})
        session.commit()
    except Exception as ex:
        session.close()
        return {'body':  [{"result": "",
                           "result-text": f"[update-DOR] - error - Заявка в релиз {application_id}. ДОР - {dor_type_id}. Содержание - {new_content} .Ошибка - {str(ex)}"
                           }]}
    session.close()
    return{'body': [{"result": "",
                     "result-text": f"[update-DOR] -info- Заявка в релиз {application_id}. ДОР - {dor_type_id}. Содержание - {new_content}"
                     }]}


@app.route('/portal/api/create-mass-feature', methods=['POST'])
def create_mass_feature(**kw):
    global Session
    session = Session()
    content = request.get_json(silent=True)
    print(content)
    jira = connect_jira()  # JIRA
    new_feature_dict = {'project': {'key': 'LINEUP'},
                        'summary': content['body'][0]['mass-feature']['name'],
                        'issuetype': 'Task',
                        'labels': ['#mass'],
                        'description': content['body'][0]['mass-feature']['content']
                        }
    try:
        new_issue_key = jira.create_issue(fields=new_feature_dict)
        mass_id = str(uuid.uuid4())
        session.add(MassFeature(mass_id,
                                new_issue_key.key,
                                content['body'][0]['mass-feature']['name'],
                                content['body'][0]['mass-feature']['type'],
                                content['body'][0]['mass-feature']['content']))
        session.commit()
    except Exception as err:
        print(f'Что-то пошло не так: {err.text}')
        return None
    finally:
        session.close()
    new_mass_feature = {'body': [{'id': mass_id, 'key': new_issue_key.key}]}
    return jsonify(new_mass_feature)

@app.route('/portal/api/get-mass-features')
def mass_features():
    global Session
    session = Session()
    try:

        mass_features = session.query(MassFeature).all()
    finally:
        session.close()

    dict_to_return = dict()
    dict_to_return['body'] = list()
    dict_in_body = dict()
    dict_in_body["user"] = "Rashitov-MT"
    dict_in_body["mass-features"] = list()  # sortedArray
    for mass_f in mass_features:
        dict_in_body["mass-features"].append({"name": mass_f.name, "type":mass_f.type, "id":mass_f.id, "key":mass_f.key})
    dict_to_return['body'].append(dict_in_body)
    return jsonify(dict_to_return)

@app.route('/portal/api/move-features', methods=['POST'])
def move_features():
    resp = []
    global Session
    content = request.get_json(silent=True)
    try:
        new_release_id = content['body'][0]['release-id']
        application_lst = content['body'][0]['application-id']
        if not isinstance(application_lst, list):
            application_lst = [application_lst]
        if len(application_lst) == 0:
            raise ValueError()
    except Exception as ex:
            return jsonify({'body': [{"result": 400, "result-text": "Неверные входящие параметры"}]})

    # check new release and platform
    try:
        session = Session()
        df_release = pd.read_sql(session.query(ReleaseRow).filter_by(release_id=new_release_id).statement, engine)
        if df_release.shape[0] == 0:
            raise ValueError(f"Релиз {new_release_id} не найден")
        new_release_key = df_release['jira_key'].iloc[0]
        new_platform = df_release['platform'].iloc[0]

        df_request_release = pd.read_sql(
            session.query(RequestRelease).filter(RequestRelease.application_id.in_(application_lst)).statement, engine)

        df_relationrelfeature = pd.read_sql(
            session.query(RelationRelFeature).filter(RelationRelFeature.application_id.in_(application_lst)).statement,
            engine)
        df_all_app_info = pd.merge(df_request_release, df_relationrelfeature,
                                   on=['application_id', 'platform', 'release_key'], how="inner")
    except Exception as ex:
            return jsonify({'body': [{"result": 500, "result-text": str(ex)}]})
    finally:
        session.close()

    try:
        jira = connect_jira()
    except Exception as ex:
        return jsonify({'body': [{"result": 500, "result-text": str(ex)}]})


    for application in application_lst:
        df_app_row = df_all_app_info[df_all_app_info['application_id'] == application]
        if df_app_row.shape[0] == 0:
            resp.append({"release-id": new_release_id, "application-id": application, "result": 400, "result-text": f"Инициатива {application} не найдена"})
            continue
        if df_app_row['platform'].iloc[0] != new_platform:
            resp.append({"release-id": new_release_id, "application-id": application, "result": 400, "result-text": "Попытка изменения платформы"})
            continue

        try:
            new_release_issue = jira.issue(new_release_key)
            old_release = df_app_row['release_key'].iloc[0]
            issue = jira.issue(df_app_row['story_key'].iloc[0])
            # Сначала удалим связь со старым релизом
            df_issue_links = pd.json_normalize(issue.raw, record_path=['fields', 'issuelinks'])
            if 'outwardIssue.key' not in df_issue_links.columns:
                df_issue_links['outwardIssue.key'] = np.NaN
            if 'inwardIssue.key' not in df_issue_links.columns:
                df_issue_links['inwardIssue.key'] = np.NaN
            df_old_release_link = df_issue_links[
                (df_issue_links['outwardIssue.key'] == old_release) | (
                            df_issue_links['inwardIssue.key'] == old_release)]
            if df_old_release_link.shape[0] > 0:
                old_link_id = df_old_release_link['id'].iloc[0]
                jira.delete_issue_link(old_link_id)
            # Создадим связь с новым релизом
            issue = jira.issue(df_app_row['story_key'].iloc[0])
            jira.create_issue_link('is part of', issue, new_release_issue, None)
        except Exception as ex:
            resp.append({"release-id": new_release_id, "application-id": application, "result": 500, "result-text": str(ex)})
            continue

        try:
            session = Session()
            for req in session.query(RequestRelease).filter_by(application_id=application).all():
                req.release_key = new_release_key
                req.release_id = new_release_id
            for rel in session.query(RelationRelFeature).filter_by(application_id=application).all():
                rel.release_key = new_release_key
            session.commit()
            resp.append({"release-id": new_release_id, "application-id": application,"result": 200, "result-text": f"Инициатива {application} успешно привязана к релизу {new_release_id}"})
        except Exception as ex:
            resp.append({"release-id": new_release_id, "application-id": application, "result": 500,
                                      "result-text": f"Инициатива {application} успешно привязана к релизу {new_release_id} в Jira, но при обновлении БД произошла ошибка:{str(ex)}"})
            continue
        finally:
            session.close()
    print(resp)
    return jsonify({'body': resp})

@app.route('/portal/api/dashbord/update-release-info', methods=['POST'])
def update_release_info():
    global Session
    content = request.get_json(silent=True)
    if 'body' not in content:
        return jsonify({'body': [{"result": "", "result-text": "Неверные входящие параметры"}]})

    df_dates = pd.DataFrame()
    df_query = pd.json_normalize(content['body'])

    if 'name' not in df_query.columns:
        df_query['name'] = np.NaN
    if 'status' not in df_query.columns:
        df_query['status'] = np.NaN
    if True in ['dates' in query_column for query_column in df_query.columns.str.split('.')]:
        df_query.rename(columns=lambda x: x.split('.')[-1], inplace=True)
        df_query = pd.melt(df_query, id_vars=['id', 'status', 'name'], var_name='date_type', value_name='date')
        df_dates = df_query[['id', 'date_type', 'date']].dropna(subset=['date_type', 'date']).sort_values(by='id')
    df_releases = df_query[['id', 'status', 'name']].drop_duplicates(subset=['id'], keep="first")
    release_lst = df_releases['id'].to_list()
    try:
        session = Session()
        # Обновление данных по релизам
        for ind, row in df_releases.iterrows():
            release_dct = row.to_dict()
            [release_dct.pop(key) for key in list(release_dct) if (pd.isna(release_dct[key]) == True) | (key == 'id')]
            if len(release_dct) > 0:
                session.query(ReleaseRow).filter_by(release_id=row.id).update(release_dct)
        session.commit()

        # Обновление данных по релизным датам
        if df_dates.shape[0] > 0:
            # Проверка корректности формата дат
            df_dates['date'] = pd.to_datetime(df_dates['date'], format='%Y-%m-%d')
            df_dates['date'] = df_dates['date'].dt.strftime('%Y-%m-%d')
            df_dates.rename(columns={"id": "release_id"}, inplace=True)
            # Подтянем платформу к датам
            df_releases = pd.read_sql(session.query(ReleaseRow.release_id, ReleaseRow.platform).filter(
                ReleaseRow.release_id.in_(release_lst)).statement, engine)
            df_dates = pd.merge(df_dates, df_releases, on='release_id', how="inner")
            # Обновление дат в БД
            insert_stmt = insert(ReleaseDates).values(df_dates.to_dict(orient='records'))
            on_conflict_stmt = insert_stmt.on_duplicate_key_update(date=insert_stmt.inserted.date, status='U')
            session.execute(on_conflict_stmt)
            session.commit()
        return jsonify({'body': [{"result": "", "result-text": "Изменения успешно созранены"}]})
    except Exception as ex:
        return jsonify({'body': [{"result": "", "result-text": str(ex)}]})
    finally:
        session.close()



# for local testing
# @app.route('/portal/')
# @app.route('/portal')
# @app.route('/feature')
# @app.route('/')
# def feature():
#     return render_template('index.html')

# @app.route('/', defaults={'path': ''})
# @app.route('/portal/', defaults={'path':''})
# @app.route('/portal/<path:path>')

# # @app.route('/portal/<PATH:PATH>')
# def index(path):
#     True
#     print('TEST - portal', file=sys.stderr)
#     if path != "":  # path != "" and os.path.exists(current_app.root_path + "/static/" + path):
#         path_to_file = os.path.join(current_app.root_path, 'portal')
#         return send_from_directory(path_to_file, path)
#     else:
#         return render_template('index.html')
#
Session, engine = create_db_session() #tunnel.local_bind_port
# app.run()
