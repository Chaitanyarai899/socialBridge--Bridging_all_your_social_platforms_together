import requests
import psycopg2
import os
import json
from datetime import datetime
from time import *
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DB_URL')

def graphql_query(query, variables):
    url = 'https://leetcode.com/graphql/'
    response = requests.post(url, json={'query': query, 'variables': variables})
    return response.json()

def fetch_tag_problem_counts(username):
    query = """
    query skillStats($username: String!) {
        matchedUser(username: $username) {
            tagProblemCounts {
                advanced { tagName tagSlug problemsSolved }
                intermediate { tagName tagSlug problemsSolved }
                fundamental { tagName tagSlug problemsSolved }
            }
        }
    }
    """
    variables = {"username": username}
    return graphql_query(query, variables)

def fetch_user_profile_calendar(username, year):
    query = """
    query userProfileCalendar($username: String!, $year: Int) {
        matchedUser(username: $username) {
            userCalendar(year: $year) {
                streak
                totalActiveDays
            }
        }
    }
    """
    variables = {"username": username, "year": datetime.now().year}
    return graphql_query(query, variables)

def fetch_recent_ac_submissions(username, limit=10):
    query = """
    query recentAcSubmissions($username: String!, $limit: Int!) {
        recentAcSubmissionList(username: $username, limit: $limit) {
            title
            timestamp
        }
    }
    """
    variables = {"username": username, "limit": limit}
    return graphql_query(query, variables)

def fetch_user_problem_solved(username):
    query = """
    query userProblemsSolved($username: String!) {
      allQuestionsCount {
       difficulty
       count
    }
    matchedUser(username: $username) {
      problemsSolvedBeatsStats {
         difficulty
         percentage
     }
    submitStatsGlobal {
      acSubmissionNum {
        difficulty
        count
          }
         }
       }
      }
    """
    variables = {"username": username}
    return graphql_query(query, variables)


def insert_stats_to_db(tag_data, calendar_data, total_questions_data, submissions_data, username):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Prepare the stats data
    stats_data = {
        'advanced': [],
        'intermediate': [],
        'fundamental': []
    }
    # Corrected the variable name from `data` to `tag_data`
    for level in ['advanced', 'intermediate', 'fundamental']:
        for problem in tag_data['data']['matchedUser']['tagProblemCounts'][level]:
            stats_data[level].append({
                'tagName': problem['tagName'],
                'tagSlug': problem['tagSlug'],
                'problemsSolved': problem['problemsSolved']
            })
    
    today = datetime.now().date()  # Use the current date
    streak = calendar_data['data']['matchedUser']['userCalendar']['streak']
    total_questions = total_questions_data['data']['matchedUser']['submitStatsGlobal']['acSubmissionNum'][0]["count"]
    print(total_questions)
    
    questions_done_today = []
    for submission in submissions_data['data']['recentAcSubmissionList']:
        submission_time = datetime.fromtimestamp(int(submission['timestamp']))
        if submission_time.date() == today:
            questions_done_today.append(submission['title'])
    questions_done_today_str = json.dumps(questions_done_today) if questions_done_today else "No questions done today"
    
    cur.execute("""
        INSERT INTO leetcode_stats (username, date, stats, questions_done_today, streak, total_questions) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (username, date) DO UPDATE 
        SET stats = EXCLUDED.stats, questions_done_today = EXCLUDED.questions_done_today, 
            streak = EXCLUDED.streak, total_questions = EXCLUDED.total_questions;
    """, (username, today, json.dumps(stats_data), questions_done_today_str, streak, total_questions))
    conn.commit()
    cur.close()
    conn.close()

def leetcode_stats_pipeline_endpoint(username):
    year = datetime.now().year
    tag_data = fetch_tag_problem_counts(username)
    calendar_data = fetch_user_profile_calendar(username, year)
    total_questions_data = fetch_user_problem_solved(username)
    submissions_data = fetch_recent_ac_submissions(username)
    insert_stats_to_db(tag_data, calendar_data, total_questions_data, submissions_data, username)