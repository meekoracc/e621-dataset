import requests
import asyncio
import logging
from dotenv import load_dotenv
import os
import pymongo
import pandas as pd
import json

API_URL='https://e621.net/'

POSTS_URL='posts.json'
TAGS_URL='tags.json'

client = None # type: pymongo.MongoClient
db = None # type: pymongo.database.Database
headers = {
  "User-Agent": os.getenv('HEADER_SIX_TWO_ONE')
}

#Import API key and user token
auth = None

# Remove superfluous information from the json body returned
def clean_posts(body):
  out = body['posts']
  
# Connect to the locally running mongo instance for saving records
def connect_to_mongo():
  global db
  global client

  conn_str = "mongodb://localhost:27017/" #TODO modify for future docker compose file
  client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

  try:
    logging.debug(client.server_info())
  except Exception:
    logging.error("Unable to connect.")

  db = client.get_database('sixtwoone')

  logging.debug(client.list_database_names())


#################################
## Get Tags                    ##
##                             ##
#################################
async def get_tags():
  params = {
    "limit": "320",
    "page": "a1",
    "search[hide_empty]": "true"
  }

  tags = db.get_collection('tags')

  #TODO Get all Tags and insert in to MongoDB instance
  response = requests.get(API_URL + TAGS_URL, auth=auth, headers=headers, params=params)
  response = pd.DataFrame.from_dict(response.json())

  # Clean DataFrame a bit
  response.set_index('id', inplace=True)
  response.drop(['related_tags', 'related_tags_updated_at','is_locked'], axis=1, inplace=True)

  logging.debug(len(response))
  logging.debug(response.head())
  logging.debug(response.columns)

  data_dict = response.to_dict('records')
  tags.insert_many(data_dict)

  await asyncio.sleep(1)

#################################
## Get Posts                   ##
##                             ##
#################################
async def get_posts():
  params = {
    "limit": "320",
    "tags": "",
    "page": "a1"
  }

  posts = db.get_collection('posts')

  #Request 320 oldest posts + offset
  response = requests.get(API_URL + POSTS_URL, auth=auth, headers=headers, params=params)

  # response = clean_posts(response.json())

  response = response.json()['posts']

  logging.debug("Retrieved {0} records".format(len(response)))
  logging.debug(response[0].keys())

  #TODO Insert into database (MongoDB?)

  # Requests capped at 1/sec for "sustained period" so sleep for >= 1s
  await asyncio.sleep(1)

  #TODO iterate if MAX_POSTS variable not reached or retrieved # is less than 320


  #TODO handle request code errors w/ recovery

async def main():
  load_dotenv()

  auth = (os.getenv('USER_SIX_TWO_ONE'), os.getenv('KEY_SIX_TWO_ONE'))

  #Start logger
  logging.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p', filename='retreival.log', filemode='w', level=logging.DEBUG)
  logging.info('Starting Retrieval Task')

  #Connect to local mongo instance
  connect_to_mongo()

  #Get tags
  await get_tags()

  #Get posts
  # await get_posts()

  logging.info('Retrieval Task Stopped')
  logging.shutdown()
  # client.close()

if __name__ == "__main__":
  asyncio.run(main())