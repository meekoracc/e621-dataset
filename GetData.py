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

headers = {
  "User-Agent": os.getenv('HEADER_SIX_TWO_ONE')
}

#Import API key and user token
auth = None

# Connect to the locally running mongo instance for saving records
def connect_to_mongo():
  conn_str = "mongodb://localhost:27017/" #TODO modify for future docker compose file
  client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

  try:
    logging.debug(client.server_info())
  except Exception:
    logging.error("Unable to connect.")

  db = client.get_database('sixtwoone')

  logging.debug(client.list_database_names())

  return (client, db)

#################################
## Get Tags                    ##
##                             ##
#################################
async def get_tags(db: pymongo.database.Database):
  params = {
    "limit": "320",
    "page": "a0",
    "search[hide_empty]": "true"
  }

  tags = db.get_collection('tags')
  # Get largest tag id from collection if it exists and continue from there
  try:
    largest = tags.find().sort("_id", -1).limit(1).next()['_id']

    logging.info(f'Largest id already here {largest}')

    params['page'] = 'a' + str(largest)
  except StopIteration:
    logging.info(f'Initializing Tags Collection')

  logging.info("Retrieving Tags")

  count = 0
  last_count = 320
  MAX_TAGS = int(os.getenv('MAX_TAGS'))
  # Get all Tags and insert in to MongoDB instance
  while ((count < MAX_TAGS) or (MAX_TAGS < 0)) and (last_count == 320):
    try:
      # Retrieve max number of tags possible
      response = requests.get(API_URL + TAGS_URL, auth=auth, headers=headers, params=params) #TODO Handle errors
      response.raise_for_status()
    except response.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
        raise http_err
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
        raise err
    # Create pandas df from result
    response = pd.DataFrame.from_dict(response.json())

    if len(response) == 0:
      return

    response.rename(columns={'id':'_id'}, inplace=True)

    # logging.debug(response.columns)

    # Clean DataFrame a bit
    response.drop(['related_tags', 'related_tags_updated_at',
                   'is_locked'], axis=1, inplace=True)

    logging.debug(f"Got {len(response)} records")
    # logging.debug(response.head())

    # Insert into database (MongoDB)
    last_count = insert_records(response, tags)

    count += last_count

    logging.info(f"Retrieved {count} records so far.")

    #Update params with new page
    logging.debug(f"Last index retrieved {response['_id'].max()}")
    params['page'] = 'a' + str(response['_id'].max())

    await asyncio.sleep(1)

#################################
## Get Posts                   ##
##                             ##
#################################
async def get_posts(db: pymongo.database.Database):
  params = {
    "limit": "320",
    "tags": "",
    "page": "a0"
  }

  posts = db.get_collection('posts')
  # Get largest post id from collection if it exists and continue from there
  try:
    largest = posts.find().sort("_id", -1).limit(1).next()['_id']

    logging.info(f'Largest id already here {largest}')

    params['page'] = 'a' + str(largest)
  except StopIteration:
    logging.info(f'Initializing Posts Collection')

  logging.info("Retrieving Posts")

  count = 0
  last_count = 320
  MAX_POSTS = int(os.getenv('MAX_POSTS'))
  # Get all posts and insert into MongoDB instance
  while ((count < MAX_POSTS) or (MAX_POSTS < 0)) and (last_count == 320):
    try:
      # Retrieve max number of posts possible
      response = requests.get(API_URL + POSTS_URL, auth=auth, headers=headers, params=params)
      response.raise_for_status()
    except response.HTTPError as http_err:
      logging.error(f'HTTP error occurred: {http_err}')
      raise http_err
    except Exception as err:
      print(f'Other error occurred: {err}')
      raise err
    # Create pandas df from result
    response = pd.DataFrame.from_dict(response.json()['posts'])

    if len(response) == 0:
      return

    response.rename(columns={'id':'_id'}, inplace=True)

    logging.debug(f"Got {len(response)} records")

    # Clean DataFrame
    clean_posts(response)

    # Insert into database (MongoDB?)
    # last_count = insert_records(response, posts)

    count += last_count

    logging.info(f"Retrieved {count} records so far.")

    # Update params with new page
    logging.debug(f"Last index retrieved {response['_id'].max()}")
    params['page'] = 'a' + str(response['_id'].max())
    # Requests capped at 1/sec for "sustained period" so sleep for >= 1s

    await asyncio.sleep(1)
    break;

    #Iterate if MAX_POSTS variable not reached or retrieved # is less than 320


  #TODO handle request code errors w/ recovery

# Remove superfluous information from the json body returned
def clean_posts(body: pd.DataFrame):
  body.drop(
    ['sample', 'sources', 'pools', 
     'relationships', 'approver_id',
     'uploader_id', 'description', 
     'comment_count', 'is_favorited', 
     'has_notes', 'duration', 'preview',
     'change_seq', 'flags', 'locked_tags'], axis=1, inplace=True)
  logging.debug(body.columns)

  # Clean file part
  logging.debug(f'File:\n{body["file"].iloc[0]}')
  body['file'] = body['file'].astype(object)
  body['url'] = [row['url'] for row in body['file']]
  body['dims'] = [{'width': row['width'], 'height':row['height']} for row in body['file']]
  body.drop(['file'], axis=1, inplace=True)

  #TODO: Clean and reduce tags (e.g. use id and separate by category)
  logging.debug(f'Tags:\n{body["tags"].iloc[0]}')
  body['tags'] = body['tags'].astype(object)

def insert_records(df: pd.DataFrame, collection: pymongo.collection.Collection):
  try:
    data_dict = df.to_dict('records')
    logging.debug("Records\n" + str(data_dict[0]))
    out = collection.insert_many(data_dict)
  except Exception as err:
    # TODO: Handle duplicate id
    logging.error("An error occured while inserting into the database.")
    logging.error(err)
    raise err
  logging.info("Inserted {0} records".format(len(out.inserted_ids)))
  return len(out.inserted_ids)


async def main():
  load_dotenv()

  auth = (os.getenv('USER_SIX_TWO_ONE'), os.getenv('KEY_SIX_TWO_ONE'))

  #Start logger
  logging.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p', filename='retreival.log', filemode='w', level=logging.DEBUG)
  logging.info('Starting Retrieval Task')

  #Connect to local mongo instance
  client, db = connect_to_mongo()

  try:
    #Get tags
    await get_tags(db)
    #Get posts
    await get_posts(db)
  finally:
    client.close()
    logging.info('Retrieval Task Stopped')
    logging.shutdown()
  # client.close()

if __name__ == "__main__":
  asyncio.run(main())