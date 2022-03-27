from sqlalchemy import create_engine, MetaData, insert, Table, delete, select
from sqlalchemy_utils import database_exists
from sqlalchemy.orm import sessionmaker
import sys
import pandas as pd
from headlines import Responses, HeadlineInfo, Workers, AssessmentHeadlines
import traceback
import math

assessment_headline_consensus = 0.8

engine = create_engine("postgres://localhost/headlines")

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
        worker_id = worker_id, 
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

def add_assessment_headline(consensus_class, headline_id, company_1, company_2, confidence_score):
    session.add(AssessmentHeadlines(
        consensus_class = consensus_class,
        headline_id = headline_id,
        company_1 = company_1,
        company_2 = company_2,
        confidence_score = confidence_score
    )) 
    session.commit()

def add_worker(prolific_id):
    session.add(Workers(
        prolific_id = prolific_id,
        trust_score = 1
    ))
    session.commit()

def update_worker_score():
    pass 

def update_assessment_headline():
    pass 

def determine_consensus(response_results):
    n = len(response_results)
    num_agree_needed = math.ceil(assessment_headline_consensus * n)
    response_class = [i.responses_response_class for i in response_results]
    most_freq_class = max(set(response_class), key = response_class.count)
    company_1_counts = {}
    company_2_counts = {}
    for r in response_results:
        if r.responses_response_class == most_freq_class:
            curr_c1 = r.responses_company_1
            curr_c2 = r.responses_company_2
            if curr_c1 in company_1_counts:
                company_1_counts[curr_c1] += 1
            else:
                company_1_counts[curr_c1] = 1

            if curr_c2 in company_2_counts:
                company_2_counts[curr_c2] += 1
            else:
                company_2_counts[curr_c2] = 1

    if not bool(company_1_counts) or not bool(company_2_counts):
        return False, None

    most_freq_company_1 = max(company_1_counts, key = company_1_counts.get)
    most_freq_company_2 = max(company_2_counts, key = company_2_counts.get)

    num_agreed = min(company_1_counts[most_freq_company_1], company_2_counts[most_freq_company_2])
    if num_agreed >= num_agree_needed:
        return True, [most_freq_class, most_freq_company_1, most_freq_company_2, num_agreed / num_agree_needed]
    return False, None

def add_responses(response_data):
    for i, response in  response_data.iterrows():
        curr_prolific_id = response['prolific_id']
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

        curr_headline_id = session.execute(headline_query).first().headline_info_headline_id

        # find worker by prolific id
        worker_query = session.query(Workers).filter(Workers.prolific_id == curr_prolific_id)
        worker_row = session.execute(worker_query).first()
        if worker_row == None:
            # add worker to workers table
            add_worker(curr_prolific_id)

        curr_worker_id = session.execute(worker_query).first().workers_worker_id

        # record response 
        add_response(curr_worker_id, curr_headline_id, curr_response_class, curr_company_1, curr_company_2)

        # check if headline is already an assessment headline
        assessment_headline_query = session.query(AssessmentHeadlines).filter(AssessmentHeadlines.headline_id == curr_headline_id)
        assessment_headline_row = session.execute(assessment_headline_query).first()
        if assessment_headline_row == None:
            # determine if there's enough of a consensus for the headline => assessment headline
            response_query = session.query(Responses).filter(Responses.headline_id == curr_headline_id)
            response_results = list(session.execute(response_query))
            is_consensus, consensus_info = determine_consensus(response_results)
            if is_consensus:
                # add to assessment headlines table
                consensus_class, company_1, company_2, confidence_score = consensus_info
                add_assessment_headline(consensus_class, curr_headline_id, company_1, company_2, confidence_score)
        else:
            # update assessment headline confidence score
            confidence_score = assessment_headline_row.assessment_headlines_confidence_score
            num_times_displayed = session.execute(headline_query).first().headline_info_num_times_displayed
            prev_num_times_displayed = num_times_displayed - 1
            curr_weighted_confidence = int(confidence_score * prev_num_times_displayed)
            next_confidence_score = (curr_weighted_confidence + 1) / num_times_displayed

            # if score dips below assessment threshold, remove from table
            if next_confidence_score < assessment_headline_consensus:
                assessment_headline_query.delete()
            else:
                assessment_headline_query.update({'confidence_score': next_confidence_score})
            session.commit()

def clear_table(table):
    q = table.delete()
    q.execute()
    session.commit()

def view_table(table):
    rows = session.query(table).all()
    for r in rows:
        print(' '.join([str(i) for i in r]))

def cleanup():
    # clear all tables
    clear_table(headline_info)
    clear_table(assessment_headlines)
    clear_table(workers)
    clear_table(responses)

initial_survey_responses = pd.read_csv('./test_data/initial_survey.csv').drop(columns = ['Unnamed: 0'])
add_responses(initial_survey_responses)
print('headline_info')
view_table(headline_info)
print()
print('workers')
view_table(workers)
print()
print('responses')
view_table(responses)
print()
print('assessment_headlines')
view_table(assessment_headlines)
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
