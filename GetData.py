import requests
import pandas
import asyncio
import logging
from dotenv import load_dotenv
import os

def main():
  #Start logger
  logging.basicConfig('%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='retreival.log')
  logging.info('Starting Retrieval Task')
  #Import API key and user token
  load_dotenv()
  #TODO Start detached task

  #TODO Request 320 oldest posts + time offset

  #TODO Insert into database (MongoDB?)

  #TODO Requests capped at 2/sec so sleep for >= 0.5s

  #TODO iterate if MAXPOSTS variable reached or retrieved # is less than 320

  #TODO handle request code errors w/ recovery

  logging.info('Retrieval Task Stopped')
  logging.shutdown()

if __name__ == "__main__":
  main()