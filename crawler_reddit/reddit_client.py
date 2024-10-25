import logging
import requests
import os
from dotenv import load_dotenv

# logger setup
logger = logging.getLogger("reddit client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

load_dotenv()

class RedditClient:
    API_BASE = "https://oauth.reddit.com"

    def __init__(self):
        # Load credentials from .env file
        self.client_id = os.environ.get("REDDIT_CLIENT_ID")
        self.client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
        self.username = os.environ.get("REDDIT_USERNAME")
        self.password = os.environ.get("REDDIT_PASSWORD")
        self.user_agent = os.environ.get("REDDIT_USER_AGENT")

        # Initialize headers
        self.headers = {'User-Agent': self.user_agent}

        # Authenticate and get access token
        self.access_token = self.authenticate()

    def authenticate(self):
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {
            'grant_type': 'password',
            'username': self.username,
            'password': self.password
        }
        headers = {'User-Agent': self.user_agent}
        
        response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
        
        if response.status_code == 200:
            token = response.json()['access_token']
            logger.info("Successfully authenticated")
            return token
        else:
            logger.error(f"Failed to authenticate: {response.status_code}")
            return None

    def get_headers(self):
        return {
            'Authorization': f'bearer {self.access_token}',
            'User-Agent': self.user_agent
        }

    def get_subreddit(self, subreddit):
        url = f"{self.API_BASE}/r/{subreddit}/new.json?limit=100"
        return self.execute_request(url)

    def execute_request(self, url):
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 403:
                logger.error(f"Access forbidden for URL: {url}")
            else:
                logger.error(f"HTTP error occurred: {err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
        return None
