import requests
import pandas as pd
import time
import datetime
import coverage_new
import numpy as np
import re
from datetime import timedelta
import os

class AppmetricaApiLoader:
    def __init__(self, token, application_id):
        self.base_url = "https://api.appmetrica.yandex.ru/logs/v1/export"
        self.headers = {'Authorization': f'Bearer {token}', 'Cache-Control': 'max-age=600', 'Connection': 'close'}
        self.application_id = application_id

    def load_events_json(self, fields, date_since=str(datetime.datetime.now().replace(microsecond=0, hour=0, minute=0,
                                                                                      second=0) - timedelta(days=1)),
                         date_until=str(datetime.datetime.now().replace(microsecond=0, hour=0, minute=0, second=0)),
                         **kwargs):
        add_params = ''
        appmetric_json = ''
        for key, val in kwargs.items():
            if val:
                add_params += f'&{key}={val}'
        query = f'{self.base_url}/events.json?application_id={self.application_id}&date_since=' \
                f'{date_since}&date_until={date_until}&fields={fields}{add_params}'
        while True:
            resp2 = requests.get(query, headers=self.headers)
            if resp2.status_code == 200:
                appmetric_json = resp2.json()
                print(f'status: {resp2.status_code}')
                break
            elif str(resp2.status_code)[0] != '2':
                print(f'Error:{resp2.content}')
                break
            else:
                print(f'status: {resp2.status_code} {resp2.content}')
                time.sleep(60)
        return appmetric_json

    @staticmethod
    def group_events(appmetric_json):
        df_events = pd.DataFrame()
        if 'data' in appmetric_json:
            df_events = pd.DataFrame(appmetric_json['data'])
            # кол-во event-ов
            df_events['number_of_events'] = 1
            df_events = \
            df_events.groupby(['event_name', 'device_manufacturer', 'device_model', 'os_version', 'app_build_number'],
                              as_index=False)['number_of_events'].count()
            df_users = pd.DataFrame(appmetric_json['data'])
            df_users = df_users.sort_values(
                by=['event_name', 'device_manufacturer', 'device_model', 'os_version', 'app_build_number',
                    'android_id'])
            df_users = df_users.drop_duplicates(
                subset=['event_name', 'device_manufacturer', 'device_model', 'os_version', 'app_build_number',
                        'android_id'], keep="first")
            df_users['number_of_users'] = 1
            df_users = \
            df_users.groupby(['event_name', 'device_manufacturer', 'device_model', 'os_version', 'app_build_number'],
                             as_index=False)['number_of_users'].count()
            df_events = pd.merge(df_events, df_users,
                                 on=['event_name', 'device_manufacturer', 'device_model', 'os_version',
                                     'app_build_number'], how="inner")
        return df_events


class Testcase:

    def get_testcase_info(self, test_cases):
        # n=200 - максимальное кол-во параметров, которое можно передать в requests query
        test_cases_lst = list(test_cases)
        test_cases_chunks = list(self.divide_chunks(test_cases_lst, 200))
        testcase_info = []
        broken_testcase = []

        for chunc in test_cases_chunks:
            test_cases_str = ','.join(chunc)
            resp = requests.get(
                'https://sbtatlas.sigma.sbrf.ru/jira/rest/atm/1.0/testcase/search?query=key+IN+(' + test_cases_str + ')',
                auth=('LOGIN', 'PASSW'), verify=False)
            if resp.status_code == 200:
                if len(resp.json()) != len(chunc):
                    contains_broken = [d['key'] for d in resp.json()]
                    broken = np.setdiff1d(chunc, contains_broken)
                    broken_testcase.extend(broken)
                testcase_info.extend(resp.json())
            else:
                print(f'Error!!! {resp.content} in chunk {test_cases_str}')

        df_testcase = self.testcase_info_to_df(testcase_info)
        return df_testcase, broken_testcase

    @staticmethod
    def divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def has_cyrillic(text):
        return bool(re.search('[а-яА-Я]', text))

    @staticmethod
    def has_latin(text):
        return bool(re.search('[a-zA-Z]', text))

    @staticmethod
    def has_spec_shar(text):
        regex = re.compile('[@!#$%^&*()<>?/\|}{~:.,]')
        return bool(regex.search(text))

    def has_rubbish(self, text):
        output = any([self.has_cyrillic(text), not self.has_latin(text), self.has_spec_shar(text)])
        return output

    def clear_rubbish(self, df, fieldname):
        df[fieldname] = df[fieldname].astype(str)
        df = df[(df[fieldname] != 'nan') & (df[fieldname] != '')]
        df[fieldname] = df[fieldname].str.strip()
        df[fieldname] = df[fieldname].str.lower()
        df_no_rub = df[df[fieldname].apply(self.has_rubbish) == False]
        df_rub = df[df[fieldname].apply(self.has_rubbish) == True]
        return df_no_rub, df_rub

    @staticmethod
    def testcase_info_to_df(testcase_info):
        df_testcase = pd.DataFrame()
        for testcase in testcase_info:
            df_steps = pd.DataFrame(testcase['testScript']['steps'])
            if 'issueLinks' in testcase:
                df_steps['issueLinks'] = str(testcase['issueLinks'])
            df_steps["key"] = testcase["key"]  # тест-кейс
            df_testcase = df_testcase.append(df_steps, ignore_index=True)
        df_testcase = df_testcase[['index', 'testData', 'issueLinks', 'key']]
        return df_testcase


if __name__ == '__main__':
    REGRESSION_STORIES_JQL = " project in (ASBOL) AND issuetype = Story AND labels = Матрица_покрытия_МП_СБОЛ"
    stories = coverage_new.get_stories(REGRESSION_STORIES_JQL)
    test_cases = coverage_new.get_tests_by_stories(stories)

    testcase = Testcase()
    df_testcase, broken_testcase = testcase.get_testcase_info(test_cases)
    print(f'broken_testcase:{broken_testcase}')
    df_testcase, df_rubbish = testcase.clear_rubbish(df_testcase, 'testData')

    APPMETR_TOKEN  = 'TOKEN'
    APP_ID = '11111'
    loader = AppmetricaApiLoader(APPMETR_TOKEN, APP_ID)
    appmetric_json = loader.load_events_json(fields='event_name,device_manufacturer,device_model,os_version,'
                                                    'app_build_number,android_id',
                                  **{'app_version_name':'10.13.0'})
    df_events = loader.group_events(appmetric_json)

    df_result = pd.merge(df_testcase, df_events, left_on='testData', right_on='event_name', how="left")
    df_result['number_of_events'].fillna(0, inplace=True)
    df_result['number_of_users'].fillna(0, inplace=True)
    df_result['number_of_events'] = df_result['number_of_events'].astype(int)
    df_result = df_result[['index', 'testData', 'issueLinks', 'key', 'device_manufacturer', 'device_model',
                           'os_version', 'app_build_number', 'number_of_events', 'number_of_users']]

    path = r'%s' % os.getcwd().replace('\\', '/')
    df_result.to_excel(path+'/result.xlsx',  index=False)
    df_rubbish.to_excel(path+'/rubbish.xlsx', index=False)
