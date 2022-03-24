from sqlalchemy import create_engine, MetaData, insert
from sqlalchemy_utils import database_exists
from sqlalchemy.orm import sessionmaker
import sys
import pandas as pd
from headlines import Responses, Base

engine = create_engine("postgres://localhost/headlines")
print(database_exists(engine.url))

conn = engine.connect()
meta = MetaData(conn)
meta.reflect()

headline_info = meta.tables['headline_info']
assessment_headlines = meta.tables['assessment_headlines']
workers = meta.tables['workers']
responses = meta.tables['responses']

Session = sessionmaker(bind = engine)
session = Session()

def collect_n_headlines(n, criteria = ''):
    pass 

def add_responses(response_data):
    for i, response in  response_data.iterrows():
        curr_worker_id = response['worker_id']
        curr_response_class = response['response_class']
        curr_company_1 = response['company_1']
        curr_company_2 = response['company_2']
        session.add(Responses(
            worker_id = 0, 
            response_class = curr_response_class, 
            company_1 = curr_company_1, 
            company_2 = curr_company_2
        ))
    session.commit()

def update_worker_score():
    pass 

def update_assessment_headline():
    pass 

def add_assessment_headline():
    pass 

def clear_table(table):
    session.query(table).delete()
    session.commit()

def view_table(table):
    rows = session.query(table).all()
    print(rows)

view_table(headline_info)

# read responses from initial survey
initial_survey_responses = pd.read_csv('./test_data/initial_survey.csv').drop(columns = ['Unnamed: 0'])
add_responses(initial_survey_responses)

# view_table(headline_info)
# clear_table(headline_info)
