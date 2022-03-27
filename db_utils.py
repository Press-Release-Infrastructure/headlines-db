from sqlalchemy import create_engine, MetaData, insert, Table, delete, select
from sqlalchemy_utils import database_exists
from sqlalchemy.orm import sessionmaker
import sys
import pandas as pd
from headlines import Responses, HeadlineInfo
import traceback

assessment_headline_consensus = 0.8

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

def add_response(worker_id, headline_id, response_class, company_1, company_2):
    session.add(Responses(
        worker_id = 0, 
        headline_id = headline_id,
        response_class = response_class,
        company_1 = company_1, 
        company_2 = company_2
    ))
    session.commit()

def add_headline(headline, article_id, priority_score):
    session.add(HeadlineInfo(
        headline = headline, 
        article_id = article_id,
        num_times_displayed = 1,
        priority_score = priority_score
    ))
    session.commit()

def add_assessment_headline():
    pass 

def add_responses(response_data):
    for i, response in  response_data.iterrows():
        curr_worker_id = response['worker_id']
        curr_response_class = response['response_class']
        curr_company_1 = response['company_1']
        curr_company_2 = response['company_2']
        curr_headline = response['headline']
        curr_article_id = response['article_id']
        curr_priority_score = response['priority_score']

        # find the headline id corresponding to the current headline
        headline_query = session.query(HeadlineInfo).filter(HeadlineInfo.headline == curr_headline)
        headline_row = session.execute(headline_query).first()
        if headline_row == None:
            # add headline to headlines table
            add_headline(curr_headline, curr_article_id, curr_priority_score)
        else:
            num_times_displayed = headline_row.headline_info_num_times_displayed
            headline_query.update({'num_times_displayed': num_times_displayed + 1})
            session.commit()

        # add_response(curr_worker_id, curr_response_class, curr_company_1, curr_company_2)

        # determine if there's enough of a consensus for the headline => assessment headline
        # num_times_displayed = conn.execute(select(session.query(headline_info)).where(headline_info.c.num_times_displayed))
        # print(num_times_displayed)

def update_worker_score():
    pass 

def update_assessment_headline():
    pass 

def clear_table(table):
    q = table.delete()
    q.execute()
    session.commit()

def view_table(table):
    rows = session.query(table).all()
    print(rows)

def cleanup():
    # clear all tables
    clear_table(headline_info)
    clear_table(assessment_headlines)
    clear_table(workers)
    clear_table(responses)

initial_survey_responses = pd.read_csv('./test_data/initial_survey.csv').drop(columns = ['Unnamed: 0'])
add_responses(initial_survey_responses)
view_table(headline_info)
cleanup()

# try:
#     # read responses from initial survey
#     initial_survey_responses = pd.read_csv('./test_data/initial_survey.csv').drop(columns = ['Unnamed: 0'])
#     add_responses(initial_survey_responses)
#     view_table(headline_info)
#     cleanup()
# except Exception as e:
#     print(traceback.format_exc())
#     cleanup()
