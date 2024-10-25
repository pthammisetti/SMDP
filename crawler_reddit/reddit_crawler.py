# import logging
# from reddit_client import RedditClient
# from pyfaktory import Client, Consumer, Job, Producer
# import datetime
# import psycopg2
# from psycopg2.extras import Json
# from psycopg2.extensions import register_adapter
# from dotenv import load_dotenv

# register_adapter(dict, Json)

# logger = logging.getLogger("reddit crawler")
# logger.setLevel(logging.INFO)
# sh = logging.StreamHandler()
# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# sh.setFormatter(formatter)
# logger.addHandler(sh)

# load_dotenv()

# import os

# FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
# DATABASE_URL = os.environ.get("DATABASE_URL")


# def get_latest_timestamp(subreddit):
#     conn = psycopg2.connect(dsn=DATABASE_URL)
#     cur = conn.cursor()

#     # Fetch the latest timestamp stored for the subreddit
#     cur.execute("SELECT latest_timestamp FROM subreddit_latest WHERE subreddit = %s", (subreddit,))
#     result = cur.fetchone()

#     cur.close()
#     conn.close()

#     if result:
#         return result[0]  # Return the latest timestamp
#     else:
#         return 0  # Default to 0 if no timestamp is found


# def update_latest_timestamp(subreddit, timestamp):
#     conn = psycopg2.connect(dsn=DATABASE_URL)
#     cur = conn.cursor()

#     # Upsert the latest timestamp for the subreddit
#     cur.execute("""
#         INSERT INTO subreddit_latest (subreddit, latest_timestamp)
#         VALUES (%s, %s)
#         ON CONFLICT (subreddit) DO UPDATE SET latest_timestamp = EXCLUDED.latest_timestamp
#     """, (subreddit, timestamp))

#     conn.commit()
#     cur.close()
#     conn.close()

# import time

# def crawl_subreddit(subreddit):
#     try:
#         reddit_client = RedditClient()
#         data = reddit_client.get_subreddit(subreddit)

#         if data is None:
#             logger.error(f"Failed to fetch data from subreddit: {subreddit}")
#             return

#         conn = psycopg2.connect(dsn=DATABASE_URL)
#         cur = conn.cursor()

#         latest_timestamp = get_latest_timestamp(subreddit)

#         for post in data['data']['children']:
#             post_data = post['data']
#             post_id = post_data['id']
#             post_created_utc = post_data['created_utc']

#             if post_created_utc > latest_timestamp:
#                 # Use ON CONFLICT to skip if the post already exists
#                 q = """
#                 INSERT INTO posts (subreddit, post_id, data) 
#                 VALUES (%s, %s, %s) 
#                 ON CONFLICT (subreddit, post_id) DO NOTHING 
#                 RETURNING id
#                 """
#                 cur.execute(q, (subreddit, post_id, post_data))
#                 conn.commit()

#                 result = cur.fetchone()
#                 if result:  # Only log if a new post was inserted
#                     db_id = result[0]
#                     logger.info(f"Inserted post ID: {db_id}")
#                 else:
#                     logger.info(f"Post {post_id} in subreddit {subreddit} already exists, skipping...")

#                 # Update latest timestamp if necessary
#                 if post_created_utc > latest_timestamp:
#                     latest_timestamp = post_created_utc

#         update_latest_timestamp(subreddit, latest_timestamp)

#         cur.close()
#         conn.close()

#         # Add a delay to avoid rate limits
#         time.sleep(2)  # Sleep for 2 seconds before fetching the next subreddit

#     except requests.exceptions.HTTPError as http_err:
#         logger.error(f"HTTP error occurred: {http_err}")
#     except Exception as err:
#         logger.error(f"An error occurred while fetching data from subreddit {subreddit}: {err}")



# def schedule_crawl(subreddit):
#     with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
#         producer = Producer(client=client)
#         job = Job(jobtype="crawl-subreddit", args=(subreddit,), queue="crawl-subreddit")
#         producer.push(job)
#         run_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
#         job = Job(jobtype="crawl-subreddit", args=(subreddit,), queue="crawl-subreddit", at=str(run_at))
#         producer.push(job)


# if __name__ == "__main__":
#     subreddits = [
#         "immigration", "refugees", "migration", "immigrationlaw", "immigrants",
#         "AskImmigration", "legaladvice", "ImmigrationCanada", "ukvisa", "usvisa",
#         "VisaJourney", "AsylumSeekers", "globalmigration", "immigrationhelp",
#         "expats", "ImmigrationNews", "IRCC", "GreenCard", "ImmigrationReform", "bordersecurity"
#     ]

#     # Loop through each subreddit and crawl data
#     for subreddit in subreddits:
#         crawl_subreddit(subreddit)

#     # After crawling all subreddits, set up the Faktory consumer
#     with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
#         consumer = Consumer(client=client, queues=["crawl-subreddit"], concurrency=5)
#         consumer.register("crawl-subreddit", crawl_subreddit)
#         consumer.run()


import logging
from reddit_client import RedditClient
import time
import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from dotenv import load_dotenv

register_adapter(dict, Json)

# Setup logging
logger = logging.getLogger("reddit crawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

load_dotenv()

import os

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_latest_timestamp(subreddit):
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    # Fetch the latest timestamp stored for the subreddit
    cur.execute("SELECT latest_timestamp FROM subreddit_latest WHERE subreddit = %s", (subreddit,))
    result = cur.fetchone()

    cur.close()
    conn.close()

    if result:
        return result[0]  # Return the latest timestamp
    else:
        return 0  # Default to 0 if no timestamp is found


def update_latest_timestamp(subreddit, timestamp):
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    # Upsert the latest timestamp for the subreddit
    cur.execute("""
        INSERT INTO subreddit_latest (subreddit, latest_timestamp)
        VALUES (%s, %s)
        ON CONFLICT (subreddit) DO UPDATE SET latest_timestamp = EXCLUDED.latest_timestamp
    """, (subreddit, timestamp))

    conn.commit()
    cur.close()
    conn.close()


def crawl_subreddit(subreddit):
    try:
        reddit_client = RedditClient()
        data = reddit_client.get_subreddit(subreddit)

        if data is None:
            logger.error(f"Failed to fetch data from subreddit: {subreddit}")
            return

        conn = psycopg2.connect(dsn=DATABASE_URL)
        cur = conn.cursor()

        latest_timestamp = get_latest_timestamp(subreddit)

        for post in data['data']['children']:
            post_data = post['data']
            post_id = post_data['id']
            post_created_utc = post_data['created_utc']

            if post_created_utc > latest_timestamp:
                # Use ON CONFLICT to skip if the post already exists
                q = """
                INSERT INTO posts (subreddit, post_id, data) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (subreddit, post_id) DO NOTHING 
                RETURNING id
                """
                cur.execute(q, (subreddit, post_id, post_data))
                conn.commit()

                result = cur.fetchone()
                if result:  # Only log if a new post was inserted
                    db_id = result[0]
                    logger.info(f"Inserted post ID: {db_id}")
                else:
                    logger.info(f"Post {post_id} in subreddit {subreddit} already exists, skipping...")

                # Update latest timestamp if necessary
                if post_created_utc > latest_timestamp:
                    latest_timestamp = post_created_utc

        update_latest_timestamp(subreddit, latest_timestamp)

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error occurred in subreddit {subreddit}: {e}")


if __name__ == "__main__":
    # subreddits = ["legaladvice","immigration","expats","refugees","ImmigrationCanada"
        
    # ]
    subreddits = [
    "travel",
    "IWantOut",
    "worldnews",
    "news",
    "politics",
    "legaladviceuk",
    "AmericanPolitics",
    "CanadaPolitics",
    "ImmigrationLawyers",
    "ExpatFinance",
    "solotravel",
    "expatriates",
    "migrationpolicy",
    "internationalrelations",
    "AskAnAmerican",
    "UnitedKingdom",
    "VisaConsultants",
    "PoliticalDiscussion",
    "AustraliaVisa",
    "ImmigrationDebate"
]


    while True:
        # Loop through each subreddit and crawl data every minute
        for subreddit in subreddits:
            crawl_subreddit(subreddit)

        logger.info("Waiting for 1 minute before fetching again...")
        time.sleep(60)  # Sleep for 1 minute before fetching again
