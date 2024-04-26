import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# User configuration
owner = os.environ.get("REPO_OWNER")
repo = os.environ.get("REPO_NAME")
token = os.environ.get("GIT_TOKEN")

def get_repo_info(owner, repo, token):
    # GitHub API URL for the repository
    url = f"https://api.github.com/repos/{owner}/{repo}"
    
    # Headers with personal access token
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Make a request to the GitHub API
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # Parse the JSON response
        repo_info = response.json()
        
        # Print basic repository information
        print("Repository Information:")
        print(f"Name: {repo_info['name']}")
        print(f"Owner: {repo_info['owner']['login']}")
        print(f"Description: {repo_info.get('description', 'No description provided.')}")
        print(f"Stars: {repo_info['stargazers_count']}")
        print(f"Forks: {repo_info['forks_count']}")
        print(f"Open Issues: {repo_info['open_issues_count']}")
        print(f"Clone URL: {repo_info['clone_url']}")
        print(f"Created at: {repo_info['created_at']}")
        print(f"Updated at: {repo_info['updated_at']}")
    else:
        print(f"Failed to retrieve repository information: {response.status_code}")

# Fetch repository information
get_repo_info(owner, repo, token)