from sqlalchemy import Column, String, Integer, BOOLEAN, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LogTable(Base):
    __tablename__ = 'all_json'
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    one_string = Column('feature_id', String(1000))

    def __init__(self, one_string):
        self.one_string = one_string


class FeatureDescriptionRow(Base):
    __tablename__ = 'mega_features_description'
    feature_id = Column('feature_id', String(50), primary_key=True)
    name = Column('name', String(1000))
    user = Column('user', String(50))
    created = Column('created', String(50))
    updated = Column('updated', String(50))

    def __init__(self, feature_id, name, user, created, updated):
        self.feature_id = feature_id
        self.name = name
        self.user = user
        self.created = created
        self.updated = updated


class RequestRelease(Base):
    __tablename__ = 'requests_to_release'
    application_id = Column('application_id', String(50), primary_key=True)
    application_key = Column('application_key', String(50))
    feature_id = Column('feature_id', String(50))
    platform = Column('platform', String(50))
    release_id = Column('release_id', String(50))
    release_key = Column('release_key', String(50))
    segment = Column('segment', String(50))
    request_type = Column('request_type', String(50))

    def __init__(self,application_id, application_key, feature_id, platform, release_id, release_key, segment, request_type):
        self.application_id = application_id
        self.application_key = application_key #new
        self.feature_id = feature_id
        self.platform = platform
        self.release_id = release_id
        self.release_key = release_key
        self.segment = segment
        self.request_type = request_type


class RelationRelFeature(Base):
    __tablename__ = 'relation_release_feature'
    application_id = Column('application_id', String(50), primary_key=True)
    platform = Column('platform', String(50))
    feature_name = Column('feature_name', String(50))
    story_key = Column('story_key', String(50))
    release_key = Column('release_key', String(50))
    type = Column('type', String(50))
    result = Column('result', String(50))
    error_code = Column('error_code', String(50))
    last_update = Column('last_update', String(50))

    def __init__(self, application_id, platform, feature_name, release_key, story_key, type, result, error_code,
                 last_update):
        self.application_id = application_id
        self.platform = platform
        self.feature_name = feature_name
        self.release_key = release_key
        self.story_key = story_key
        self.type = type
        self.result = result
        self.error_code = error_code
        self.last_update = last_update


class RelationSwellFeature(Base):
    __tablename__ = 'relation_swell_feature'
    application_id = Column('application_id', String(50), primary_key=True)
    platform = Column('platform', String(50))
    feature_name = Column('feature_name', String(50))
    swell_key = Column('swell_key', String(50))
    release_key = Column('release_key', String(50))
    request_type = Column('request_type', String(50))
    result = Column('result', String(50))
    error_code = Column('error_code', String(50))
    last_update = Column('last_update', String(50))

    def __init__(self, application_id, platform,  feature_name, swell_key, release_key, request_type, result, error_code,
                 last_update):
        self.application_id = application_id
        self.platform = platform
        self.feature_name = feature_name
        self.swell_key = swell_key
        self.release_key = release_key
        self.request_type = request_type
        self.result = result
        self.error_code = error_code
        self.last_update = last_update


class ReleaseRow(Base):
    __tablename__ = 'release'
    release_id = Column('release_id', String(50), primary_key=True)  # Уникальный идентификатор релиза
    name = Column('name', String(50))  # Наименование релиза
    platform = Column('platform', String(50))  # Платформа
    jira_key = Column('jira_key', String(50))  # Ключ релиза в jira


    def __init__(self, release_id, name, platform, jira_key ):
        self.release_id = release_id
        self.name = name
        self.platform = platform
        self.jira_key = jira_key


class ReleaseDates(Base):  # Даты релизов
    __tablename__ = 'releasedates'
    release_id = Column('release_id', String(50), primary_key=True)
    date_type = Column('date_type', String(50), primary_key=True)
    platform = Column('platform', String(50))  # Платформа
    date = Column('date', String(50))

    def __init__(self, release_id, platform, date_type, date):
        self.release_id = release_id
        self.platform = platform
        self.date_type = date_type
        self.date = date



        
class EribRequest(Base):
    __tablename__ = 'erib_table'
    feature_id = Column('feature_id', String(50),  primary_key=True)

    def __init__(self, feature_id):
        self.feature_id = feature_id
        
        

class DOR(Base):
    __tablename__ = 'dor'
    application_id = Column('application_id', String(50), primary_key=True)
    subtask_key = Column('subtask_key', String(50), primary_key=True)
    dor_type_id = Column('dor_type_id',  String(50))
    content = Column('content',  String(200))
    status = Column('status', String(50))
    user = Column('user', String(50))
    assignee_key = Column('assignee_key', String(50))
    assignee_name = Column('assignee_name', String(50))
    assignee_email = Column('assignee_email', String(50)) 
    last_update = Column('last_update', String(50))

    def __init__(self, application_id, subtask_key, dor_type_id,  content, status,  user, assignee_key, assignee_name, assignee_email,  last_update):
        self.application_id = application_id
        self.subtask_key = subtask_key
        self.dor_type_id = dor_type_id
        self.content = content
        self.status = status
        self.user = user
        self.assignee_key = assignee_key
        self.assignee_name = assignee_name
        self.assignee_email = assignee_email
        self.last_update = last_update
        
           
        
class DORInformation(Base):
    __tablename__ = 'dor_info' 
    dor_type_id = Column('dor_type_id', String(50), primary_key=True)
    category =  Column('category', String(500))
    dor_name = Column('dor_name', String(1000))
    short_name = Column('short_name', String(100))
    short_description = Column('short_description', String(1000))
    upper_text = Column('upper_text', String(1000))
    description = Column('description', String(1000))
    placeholder = Column('placeholder', String(1000))
    label = Column('label', String(50))
    dor_type = Column('dor_type', String(50))
    close_date = Column('close_date', String(50))
    close_text = Column('close_text', String(1000))
    show_in_details = Column("show_in_details" , String(50))
    # json_label = Column('json_label', String(50))

    def __init__(self, dor_type_id, category,  dor_name, short_name, short_description, upper_text, description, placeholder, label,  dor_type, close_date, close_text,  show_in_details):
        self.dor_type_id = dor_type_id
        self.category = category
        self.dor_name = dor_name
        self.short_name = short_name
        self.short_description = short_description
        self.upper_text = upper_text
        self.description = description
        self.placeholder = placeholder
        self.label = label
        self.dor_type =  dor_type
        self.close_date = close_date
        self.close_text = close_text
        self.show_in_details = show_in_details
          
class Bugs(Base):
    __tablename__ = 'bugs' 
    key = Column('key', String(50), primary_key=True)
    platform = Column('platform', String(50), primary_key=True)
    project = Column('project', String(50))
    status =  Column('status', String(50))
    name = Column('name', String(500))
    priority = Column('priority', String(50))
    def __init__(self, key, platform, project, status, name,  priority):
        self.key = key
        self.platform = platform
        self.project = project
        self.status = status
        self.name = name
        self.priority = priority
        
class TeamBugs(Base):
    __tablename__ = 'team_bugs' 
    project = Column('project', String(50), primary_key=True)
    platform = Column('platform', String(50), primary_key=True)
    priority = Column('priority', String(50), primary_key=True)
    number_of_bugs = Column('number_of_bugs', String(50))
    def __init__(self, project, platform, priority, number_of_bugs):
        self.project = project
        self.platform = platform
        self.priority = priority
        self.number_of_bugs = number_of_bugs


class ProjectDescription(Base):
    __tablename__ = 'project_description'
    key = Column('key', String(50), primary_key=True)
    name = Column('name', String(500))
    category = Column('category', String(50))
    description = Column('description', String(1000))

    def __init__(self, key, name, category, description):
        self.key = key
        self.name = name
        self.category = category
        self.description = description