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

def get_contributors(owner, repo, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contributors"
    contributors = []
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    params = {'per_page': 100}
    count = 0

    while url:
        params["page"] = count
        print(count)
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            batch = response.json()
            contributors.extend(batch)
            if len(batch) < 100:  # Check if less than 100 contributors were returned
                break  # Less than 100 contributors indicates the last page
            count += 1
        else:
            print(f"Failed to fetch contributors: {response.status_code}")
            break

    return contributors

def get_contributor_details(contributors, owner, repo, token):
    details = []
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    count = 1
    
    for contributor in contributors:
        print("contributor: ", count)
        username = contributor['login']
        contributions = contributor['contributions']
        company = None
        date_first = None
        date_last = None

        # Get user details
        user_url = f"https://api.github.com/users/{username}"
        user_response = requests.get(user_url, headers=headers)
        if user_response.status_code == 200:
            user_data = user_response.json()
            company = user_data.get('company')

        # Get commit details to find first and last contribution dates
        commits_url = f"https://api.github.com/repos/{owner}/{repo}/commits?author={username}"
        commits_response = requests.get(commits_url, headers=headers)
        if commits_response.status_code == 200:
            commits = commits_response.json()
            if commits:
                date_first = commits[-1]['commit']['committer']['date']
                date_last = commits[0]['commit']['committer']['date']

        details.append({
            'github_username': username,
            'company': company,
            'total_contributions': contributions,
            'date_first_contribution': date_first,
            'date_last_contribution': date_last
        })
        
        count += 1
    
    return details

# Main function to fetch data and save it
def main():
    contributors = get_contributors(owner, repo, token)
    contributors_details = get_contributor_details(contributors, owner, repo, token)
    df = pd.DataFrame(contributors_details)
    df.to_parquet('contributors.parquet')

main()