from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine("postgres://localhost/headlines")
if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))

Base = declarative_base()

# create tables
class HeadlineInfo(Base):
    __tablename__ = 'headline_info'
    id = Column('id', Integer, primary_key = True, autoincrement = True)
    headline = Column('headline', String)
    article_id = Column('article_id', String)
    num_times_displayed = Column('num_times_displayed', Integer)
    priority_score = Column('priority_score', Float)

class AssessmentHeadlines(Base):
    __tablename__ = 'assessment_headlines'
    id = Column('id', Integer, primary_key = True, autoincrement = True)
    consensus_class = Column('consensus_class', Integer)
    company_1 = Column('company_1', String)
    company_2 = Column('company_2', String)
    confidence_score = Column('confidence_score', Float)

class Workers(Base):
    __tablename__ = 'workers'
    id = Column('id', Integer, primary_key = True, autoincrement = True)
    prolific_id = Column('prolific_id', String)
    trust_score = Column('trust_score', Float)

class Responses(Base):
    __tablename__ = 'responses'
    id = Column('id', Integer, primary_key = True, autoincrement = True)
    worker_id = Column('worker_id', Integer)
    response_class = Column('response_class', Integer)
    company_1 = Column('company_1', String)
    company_2 = Column('company_2', String)

Base.metadata.create_all(engine)
