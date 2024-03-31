import requests
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve database credentials from environment variables
DATABASE_URL = os.getenv('DB_URL')

def create_db_and_schema():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_tag_problem_counts (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            level VARCHAR(50) NOT NULL,
            tagName VARCHAR(255) NOT NULL,
            tagSlug VARCHAR(255) NOT NULL,
            problemsSolved INT NOT NULL
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def fetch_leetcode_stats(username):
    url = 'https://leetcode.com/graphql/'
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
    response = requests.post(url, json={'query': query, 'variables': variables})
    return response.json()

def insert_stats_to_db(data, username):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    for level in ['advanced', 'intermediate', 'fundamental']:
        for problem in data['data']['matchedUser']['tagProblemCounts'][level]:
            cur.execute("INSERT INTO leetcode_stats (username, level, tagName, tagSlug, problemsSolved) VALUES (%s, %s, %s, %s, %s)",
                        (username, level, problem['tagName'], problem['tagSlug'], problem['problemsSolved']))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_db_and_schema()
    username = "chaitanyarai"
    data = fetch_leetcode_stats(username)
    insert_stats_to_db(data, username)
