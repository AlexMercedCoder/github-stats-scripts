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
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/commit_activity"
    
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
        
        print(repo_info)
        

# Fetch repository information
get_repo_info(owner, repo, token)