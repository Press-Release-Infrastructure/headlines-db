from db_utils import populate_headlines, add_responses, view_table, collect_headlines, cleanup
from db_utils import HeadlineInfo, Workers, Responses, AssessmentHeadlines
import pandas as pd 
import hashlib

acq_thresh = 3

headline = []
article_id = []
priority_score = []
likely_acquisition = []
lexis_nexis = []

with open('./headlines_input/headlines_no_duplicates.txt') as headlines_file:
    for line in headlines_file.readlines():
        try:
            curr_headline, curr_priority_score, curr_article_id = [i.strip() for i in line.split(" || ")]
            headline.append(curr_headline)
            article_id.append(curr_article_id)
            priority_score.append(curr_priority_score)
            lexis_nexis.append(1)
            
            curr_likely_acquisition = 1 if float(curr_priority_score) < acq_thresh else 0
            likely_acquisition.append(curr_likely_acquisition)
        except:
            continue

crunchbase_headlines = pd.read_csv('./headlines_input/crunchbase_headlines.csv')
for ch in crunchbase_headlines['Headline']:
    headline.append(ch)
    article_id.append(int(hashlib.sha256(ch.encode('utf-8')).hexdigest(), 16) % 10**32)
    priority_score.append(-1)
    likely_acquisition.append(1)
    lexis_nexis.append(0)

headline_data = pd.DataFrame({
    'headline': headline,
    'article_id': article_id,
    'likely_acquisition': likely_acquisition,
    'lexis_nexis': lexis_nexis,
})

# cleanup()

# populate_headlines(headline_data)

# view_table(HeadlineInfo)
# view_table(Workers)
# view_table(Responses)
# view_table(AssessmentHeadlines)

collect_headlines(500, 0, './survey_inputs/headlines.csv', './survey_inputs/assessment.csv', criteria = 'include_crunchbase')

# cleanup()
