import os
import requests
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv('DB_URL')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Setup headers for authenticated requests
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def fetch_user_repos(username):
    """Fetch all repositories for a user with authentication"""
    repos_url = f'https://api.github.com/users/{username}/repos?per_page=100'
    response = requests.get(repos_url, headers=HEADERS)
    return response.json() if isinstance(response.json(), list) else []

def fetch_commits_today(username, repo):
    """Fetch commits from today for a repository"""
    today = datetime.now().date().isoformat()
    commits_url = f'https://api.github.com/repos/{username}/{repo}/commits?since={today}T00:00:00Z'
    response = requests.get(commits_url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch commits: {response.text}")
        return []

    data = response.json()
    if not isinstance(data, list) or not all(isinstance(commit, dict) for commit in data):
        print(f"Unexpected data format: {data}")
        return []

    return [commit for commit in data if commit['commit']['author']['date'].startswith(today)]

def analyze_user(username):
    repos = fetch_user_repos(username)
    today = datetime.now().date()
    commits_today = []
    total_open_issues = 0
    total_open_prs = 0

    for repo in repos:
        today_commits = fetch_commits_today(username, repo['name'])
        if today_commits:
            commits_today.append({repo['name']: len(today_commits)})

        issues_url = f"https://api.github.com/repos/{username}/{repo['name']}/issues?state=open"
        issues = requests.get(issues_url, headers=HEADERS).json()
        total_open_issues += sum(1 for issue in issues if 'pull_request' not in issue)

        prs_url = f"https://api.github.com/repos/{username}/{repo['name']}/pulls?state=open"
        pull_requests = requests.get(prs_url, headers=HEADERS).json()
        total_open_prs += len(pull_requests) if isinstance(pull_requests, list) else 0

    save_to_database(username, today, commits_today, total_open_issues, total_open_prs, len(repos))

def save_to_database(username, date, commits_today, open_issues, open_pull_requests, total_repos):
    """Insert or update the GitHub stats in the database"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS github_stats (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            commits_today JSONB NOT NULL,
            open_issues INT NOT NULL,
            open_pull_requests INT NOT NULL,
            total_repos INT NOT NULL,
            UNIQUE(username, date)
        );
    """)
    conn.commit()

    cursor.execute("""
        INSERT INTO github_stats (username, date, commits_today, open_issues, open_pull_requests, total_repos)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (username, date)
        DO UPDATE SET
            commits_today = EXCLUDED.commits_today,
            open_issues = EXCLUDED.open_issues,
            open_pull_requests = EXCLUDED.open_pull_requests,
            total_repos = EXCLUDED.total_repos;
    """, (username, date, Json(commits_today), open_issues, open_pull_requests, total_repos))
    conn.commit()
    cursor.close()
    conn.close()

def insert_into_db(username):
    pass
