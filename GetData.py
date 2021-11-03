import requests
import asyncio
import logging
from dotenv import load_dotenv
import os
import json

API_URL='https://e621.net/'

POSTS_URL='posts.json'
TAGS_URL='tags.json'

async def main():
  load_dotenv()

  #Start logger
  logging.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p', filename='retreival.log', filemode='w', level=logging.DEBUG)
  logging.info('Starting Retrieval Task')

  #TODO Connect to local mongo instance

  #TODO Get tags

  #Get posts
  await get_posts()

  logging.info('Retrieval Task Stopped')
  logging.shutdown()

def clean_posts(body):
  out = body['posts']
  

async def get_posts():
  headers = {
    "User-Agent": os.getenv('HEADER_SIX_TWO_ONE')
  }

  #Import API key and user token
  auth = (os.getenv('USER_SIX_TWO_ONE'), os.getenv('KEY_SIX_TWO_ONE'))

  #TODO Get all Tags and insert in to MongoDB instance
  # response = requests.get(POSTS_URL + TAGS_URL, auth=auth)

  params = {
    "limit": "320",
    "tags": "",
    "page": "a1"
  }

  #Request 320 oldest posts + offset
  response = requests.get(API_URL + POSTS_URL, auth=auth, headers=headers, params=params)

  # response = clean_posts(response.json())

  response = response.json()['posts']

  logging.debug("Retrieved {0} records".format(len(response)))
  logging.debug(response[0].keys())

  #TODO Insert into database (MongoDB?)

  # Requests capped at 2/sec so sleep for >= 0.5s
  await asyncio.sleep(1)

  #TODO iterate if MAX_POSTS variable not reached or retrieved # is less than 320


  #TODO handle request code errors w/ recovery

if __name__ == "__main__":
  asyncio.run(main())