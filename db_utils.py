from sqlalchemy import create_engine, MetaData, insert, Table, delete, select
from sqlalchemy_utils import database_exists
from sqlalchemy.orm import sessionmaker
import sys
import pandas as pd
from headlines import Responses, HeadlineInfo, Workers, AssessmentHeadlines
from sqlalchemy.sql.expression import func
import traceback
import math

assessment_headline_consensus = 0.8
assessment_headline_threshold_num = 2

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

def collect_headlines(num_headlines, num_assessment, headlines_out, assessment_out, criteria = 'high_priority'):
    # select n headlines with the highest priority values from the headlines table
    headline_query = session.query(HeadlineInfo).order_by(HeadlineInfo.priority_score.desc()) 
    headline_result = session.execute(headline_query)
    count = 0
    headlines_used = []
    for h in headline_result:
        headlines_used.append(h.headline_info_headline)
        count += 1
        if count >= num_headlines:
            break
    
    assessment_headline_query = session.query(AssessmentHeadlines).order_by(func.random())
    assessment_headline_result = session.execute(assessment_headline_query)
    count = 0
    assessment_headlines_used = []
    assessment_headlines_cc = []
    assessment_headlines_c1 = []
    assessment_headlines_c2 = []
    for a in assessment_headline_result:
        curr_assessment_headline = get_attribute(HeadlineInfo, HeadlineInfo.headline_id == a.assessment_headlines_headline_id, 'headline_info_headline')
        curr_assessment_consensus_class = a.assessment_headlines_consensus_class 
        curr_assessment_company_1 = a.assessment_headlines_company_1
        curr_assessment_company_2 = a.assessment_headlines_company_2
        
        assessment_headlines_used.append(curr_assessment_headline)
        assessment_headlines_cc.append(curr_assessment_consensus_class)
        assessment_headlines_c1.append(curr_assessment_company_1)
        assessment_headlines_c2.append(curr_assessment_company_2)
        count += 1
        if count >= num_headlines:
            break 

    headlines_df = pd.DataFrame({
        'Headline': headlines_used
    })

    assessment_headlines_df = pd.DataFrame({
        'Title': assessment_headlines_used,
        'Acq_Status': assessment_headlines_cc,
        'Company 1': assessment_headlines_c1,
        'Company 2': assessment_headlines_c2
    })
    
    headlines_df.to_csv(headlines_out)
    assessment_headlines_df.to_csv(assessment_out)

def add_response(worker_id, headline_id, response_class, company_1, company_2):
    session.add(Responses(
        worker_id = worker_id, 
        headline_id = headline_id,
        response_class = response_class,
        company_1 = company_1, 
        company_2 = company_2
    ))
    session.commit()

def add_headline(headline, article_id, num_times_displayed, priority_score):
    session.add(HeadlineInfo(
        headline = headline, 
        article_id = article_id,
        num_times_displayed = num_times_displayed,
        priority_score = priority_score
    ))
    session.commit()

def populate_headlines(headlines_df):
    for i, headline_info in headlines_df.iterrows():
        headline, article_id, priority_score = headline_info['headline'], headline_info['article_id'], headline_info['priority_score']
        add_headline(headline, article_id, 0, priority_score)

def add_assessment_headline(consensus_class, headline_id, company_1, company_2, confidence_score):
    session.add(AssessmentHeadlines(
        consensus_class = consensus_class,
        headline_id = headline_id,
        company_1 = company_1,
        company_2 = company_2,
        confidence_score = confidence_score
    )) 
    session.commit()

def add_worker(prolific_id, num_headlines_completed, num_assessment_headlines_completed):
    session.add(Workers(
        prolific_id = prolific_id,
        num_headlines_completed = num_headlines_completed,
        num_assessment_headlines_completed = num_assessment_headlines_completed,
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
        return True, [most_freq_class, most_freq_company_1, most_freq_company_2, num_agreed / n]
    return False, None

def update_headline(curr_headline, curr_article_id, curr_priority_score):
    # find the headline id corresponding to the current headline
    headline_query = session.query(HeadlineInfo).filter(HeadlineInfo.headline == curr_headline)
    headline_row = session.execute(headline_query).first()
    if headline_row == None:
        # add headline to headlines table
        add_headline(curr_headline, curr_article_id, 1, curr_priority_score)
    else:
        num_times_displayed = headline_row.headline_info_num_times_displayed
        headline_query.update({'num_times_displayed': num_times_displayed + 1})
        session.commit()

def get_attribute(table, condition, column):
    query = session.query(table).filter(condition)
    result = session.execute(query).first()
    return result[column]

def get_rows(table, condition, first = False):
    query = session.query(table).filter(condition)
    result = session.execute(query)
    if first:
        return result.first()
    return result

def delete_rows(table, condition):
    query = session.query(table).filter(condition)
    query.delete()

def update_attribute(table, condition, columns, new_values):
    query = session.query(table).filter(condition)
    for c in range(len(columns)):
        query.update({
            columns[c]: new_values[c]
        })

def add_responses(response_data):
    for i, response in  response_data.iterrows():
        curr_prolific_id = response['prolific_id']
        curr_response_class = response['response_class']
        curr_company_1 = response['company_1']
        curr_company_2 = response['company_2']
        curr_headline = response['headline']
        curr_article_id = response['article_id']
        curr_priority_score = response['priority_score']

        update_headline(curr_headline, curr_article_id, curr_priority_score)

        curr_headline_id = get_attribute(HeadlineInfo, HeadlineInfo.headline == curr_headline, 'headline_info_headline_id')

        # find worker by prolific id
        worker_row = get_rows(Workers, Workers.prolific_id == curr_prolific_id, first = True)
        if worker_row == None:
            # add worker to workers table
            add_worker(curr_prolific_id, 0, 0)
        num_headlines_completed = get_attribute(Workers, Workers.prolific_id == curr_prolific_id, 'workers_num_headlines_completed')
        update_attribute(Workers, Workers.prolific_id == curr_prolific_id, ['num_headlines_completed'], [num_headlines_completed + 1])
        session.commit()

        curr_worker_id = get_attribute(Workers, Workers.prolific_id == curr_prolific_id, 'workers_worker_id')

        # record response 
        add_response(curr_worker_id, curr_headline_id, curr_response_class, curr_company_1, curr_company_2)

        # check if headline is already an assessment headline
        assessment_headline_row = get_rows(AssessmentHeadlines, AssessmentHeadlines.headline_id == curr_headline_id, first = True)
        
        num_times_displayed = get_attribute(HeadlineInfo, HeadlineInfo.headline_id == curr_headline_id, 'headline_info_num_times_displayed')
        
        if assessment_headline_row == None:
            # determine if there's enough of a consensus for the headline => assessment headline
            response_results = list(get_rows(Responses, Responses.headline_id == curr_headline_id, first = False))
            is_consensus, consensus_info = determine_consensus(response_results)
            if is_consensus and num_times_displayed >= assessment_headline_threshold_num:
                # add to assessment headlines table
                consensus_class, company_1, company_2, confidence_score = consensus_info
                add_assessment_headline(consensus_class, curr_headline_id, company_1, company_2, confidence_score)
        else:
            num_assessment_headlines_completed = worker_row.workers_num_assessment_headlines_completed

            # update worker trust score based on assessment headline performance (& confidence)
            correct_consensus_class = assessment_headline_row.assessment_headlines_consensus_class
            correct_company_1 = assessment_headline_row.assessment_headlines_company_1
            correct_company_2 = assessment_headline_row.assessment_headlines_company_2

            curr_worker_trust_score = worker_row.workers_trust_score
            num_assessment_correct = curr_worker_trust_score * num_assessment_headlines_completed

            if curr_response_class == correct_consensus_class and curr_company_1 == correct_company_1 and curr_company_2 == correct_company_2:
                next_worker_trust_score = (num_assessment_correct + 1) / (num_assessment_headlines_completed + 1)
            else:
                next_worker_trust_score = num_assessment_correct / (num_assessment_headlines_completed + 1)

            update_attribute(Workers, Workers.prolific_id == curr_prolific_id, ['trust_score'], [next_worker_trust_score])
            update_attribute(Workers, Workers.prolific_id == curr_prolific_id, ['num_assessment_headlines_completed'], [num_assessment_headlines_completed + 1])

            assessment_headline_row = get_rows(AssessmentHeadlines, AssessmentHeadlines.headline_id == curr_headline_id, first = True)

            response_results = list(get_rows(Responses, Responses.headline_id == curr_headline_id, first = False))
            is_consensus, consensus_info = determine_consensus(response_results)
            assessment_update = 1
            if is_consensus:
                # check if consensus is equal to current assessment headline entry; if not, update
                new_consensus_class, new_company_1, new_company_2, new_confidence_score = consensus_info 
                if not (new_consensus_class == correct_consensus_class and new_company_1 == correct_company_1 and new_company_2 == correct_company_2):
                    update_attribute(
                        AssessmentHeadlines, 
                        AssessmentHeadlines.headline_id == curr_headline_id, 
                        ['consensus_class', 'company_1', 'company_2', 'confidence_score'], 
                        [new_consensus_class, new_company_1, new_company_2, new_confidence_score]
                    )
                else:
                    assessment_update = 0
            else:
                delete_rows(AssessmentHeadlines, AssessmentHeadlines.headline_id == curr_headline_id)
            
            # update assessment headline confidence score
            confidence_score = assessment_headline_row.assessment_headlines_confidence_score
            num_times_displayed = get_attribute(HeadlineInfo, HeadlineInfo.headline_id == curr_headline_id, 'headline_info_num_times_displayed')
            prev_num_times_displayed = num_times_displayed - 1
            curr_weighted_confidence = int(confidence_score * prev_num_times_displayed)
            next_confidence_score = (curr_weighted_confidence + assessment_update) / num_times_displayed

            # if score dips below assessment threshold, remove from table
            if next_confidence_score < assessment_headline_consensus:
                delete_rows(AssessmentHeadlines, AssessmentHeadlines.headline_id == curr_headline_id)
            else:
                update_attribute(AssessmentHeadlines, AssessmentHeadlines.headline_id == curr_headline_id, ['confidence_score'], [next_confidence_score])
            session.commit()

def clear_table(table):
    q = table.delete()
    q.execute()
    session.commit()

def view_table(table):
    tablename = table.__tablename__
    rows = session.execute(session.query(table))
    colnames = list(rows.keys())
    print(tablename)
    print('\t'.join([str(i[len(tablename) + 1:]) for i in colnames]))
    for r in rows:
        print('\t\t'.join([str(i) for i in r]))
    print()

def cleanup():
    # clear all tables
    clear_table(headline_info)
    clear_table(assessment_headlines)
    clear_table(workers)
    clear_table(responses)
