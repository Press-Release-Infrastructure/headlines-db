from db_utils import populate_headlines, add_responses, view_table, collect_headlines, cleanup
from db_utils import HeadlineInfo, Workers, Responses, AssessmentHeadlines
import pandas as pd 

initial_headlines = pd.read_csv('./headlines_input/headlines.csv')
populate_headlines(initial_headlines)

initial_survey_responses = pd.read_csv('./survey_results/initial_survey.csv').drop(columns = ['Unnamed: 0'])
add_responses(initial_survey_responses)

view_table(HeadlineInfo)
view_table(Workers)
view_table(Responses)
view_table(AssessmentHeadlines)

collect_headlines(2, 2, './survey_inputs/headlines.csv', './survey_inputs/assessment.csv', criteria = 'high_priority')

cleanup()
