import requests
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
from dotenv import load_dotenv
import os

load_dotenv()

# User configuration
owner = os.environ.get("REPO_OWNER")
repo = os.environ.get("REPO_NAME")
token = os.environ.get("GIT_TOKEN")
filename = os.environ.get("FILENAME")

def get_contributions_by_month(owner, repo, token):
    # Initialize the result dataframe
    df = pd.DataFrame(columns=['date', 'total_contributions'])
    
    # GitHub API URL for repository commits
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/commit_activity"
    
    # Headers with personal access token
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
        "X-GitHub-Api-Version" : "2022-11-28"
    }
    
    # Make a request to the GitHub API
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        new_rows = []
        
        for week in data:
            # Convert UNIX timestamp to date
            week_end = datetime.utcfromtimestamp(week['week'] + 6 * 86400)
            month_end = (week_end + MonthEnd(0)).date()
            
            # Create a row
            new_rows.append({'date': month_end, 'total_contributions': week['total']})
        
        # Concatenate new rows to the dataframe
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        df = df.groupby('date')['total_contributions'].sum().reset_index()
    else:
        print(f"Failed to retrieve data: {response.status_code}")
    
    return df

def save_to_parquet(df, filename):
    df.to_parquet(filename, index=False)

# Fetch data
contributions_df = get_contributions_by_month(owner, repo, token)

# Save data to Parquet
save_to_parquet(contributions_df, filename + "-contributions.parquet")

print("Data saved successfully to", filename)

def get_pull_requests_by_month(owner, repo, token):
    # Initialize the result dataframe
    df = pd.DataFrame(columns=['date', 'total_pull_requests'])

    # URL to fetch pull requests from GitHub API
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    
    # Headers with personal access token and request parameters
    headers = {'Authorization': f'token {token}'}
    params = {'state': 'all', 'per_page': 100}

    while url:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            pull_requests = response.json()
            new_rows = []
            
            # Process each pull request
            for pr in pull_requests:
                merged_at = pr.get('merged_at')
                if merged_at:
                    # Convert merged date to datetime and normalize to the month's end
                    merged_date = datetime.strptime(merged_at, '%Y-%m-%dT%H:%M:%SZ')
                    month_end = (merged_date + MonthEnd(0)).date()
                    
                    # Create a row
                    new_rows.append({'date': month_end, 'total_pull_requests': 1})
            
            # Concatenate new rows to the dataframe
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
            df = df.groupby('date')['total_pull_requests'].sum().reset_index()

            # Handle pagination
            link = response.headers.get('Link')
            if link and 'rel="next"' in link:
                url = link.split(';')[0].strip('<>')
            else:
                url = None
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            break

    return df

def save_to_parquet(df, filename):
    df.to_parquet(filename, index=False)

# Fetch data
pull_requests_df = get_pull_requests_by_month(owner, repo, token)

# Save data to Parquet
save_to_parquet(pull_requests_df, filename + "-pullrequests.parquet")

print("Data saved successfully to", filename)

def get_contributors_by_month(owner, repo, token):
    # Initialize the result dataframe
    df = pd.DataFrame(columns=['date', 'total_contributors'])

    # GitHub API URL for repository contributors
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/contributors"

    # Headers with personal access token
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    # Make a request to the GitHub API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        contributors = response.json()

        # Dictionary to track contributors each month
        contributors_count = {}

        for contributor in contributors:
            for week in contributor['weeks']:
                # Convert UNIX timestamp to date
                week_end = datetime.utcfromtimestamp(week['w'])
                month_end = (week_end + MonthEnd(0)).date()

                # Initialize the month in the dictionary if not present
                if month_end not in contributors_count:
                    contributors_count[month_end] = set()
                
                # Add the contributor's ID to the set of the corresponding month
                contributors_count[month_end].add(contributor['author']['id'])
        
        # Create rows from the dictionary
        new_rows = [{'date': date, 'total_contributors': len(ids)} for date, ids in contributors_count.items()]
        
        # Concatenate new rows to the dataframe
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        df.sort_values(by='date', inplace=True)
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return None

    return df

def save_to_parquet(df, filename):
    df.to_parquet(filename, index=False)

# Fetch data
contributors_df = get_contributors_by_month(owner, repo, token)

# Save data to Parquet
save_to_parquet(contributors_df, filename + "-contributors.parquet")

print("Data saved successfully to", filename)
