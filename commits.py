import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# User configuration
owner = os.getenv("REPO_OWNER", "apache")  # default to apache if not set
repo = os.getenv("REPO_NAME", "iceberg")  # default to iceberg if not set
token = os.getenv("GIT_TOKEN")  # your GitHub token

def get_commits(owner, repo, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    commits = []
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    params = {'per_page': 100}
    count = 0

    while url:
        params["page"] = count
        print("page:", count)
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            batch = response.json()
            for commit in batch:
                print(commit['commit']['message'])
                commit_data = {
                    'date': commit['commit']['author']['date'],
                    'commit_message': commit['commit']['message'],
                    'author_name': commit['commit']['author']['name'],
                    'author_email': commit['commit']['author']['email']
                }
                commits.append(commit_data)
            if len(batch) < 100:
                break  # Break if less than 100 commits, indicating the last page
            count += 1
        else:
            print(f"Failed to fetch commits: {response.status_code}")
            break

    return commits

def save_to_parquet(data, filename):
    df = pd.DataFrame(data)
    df.to_parquet(filename)

def main():
    commits = get_commits(owner, repo, token)
    save_to_parquet(commits, 'commits.parquet')
    print("Data saved successfully to commits.parquet")

main()