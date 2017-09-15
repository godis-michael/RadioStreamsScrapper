from datetime import datetime

import psycopg2
from psycopg2 import sql
import sys
from iradio import RadioStreamsScrapper
import numpy as np

example = RadioStreamsScrapper('https://www.internet-radio.com/')
categories = example.load_categories()

schema_stamp = '{:%Y-%b-%d %H:%M}'.format(datetime.now())
schema = example.create_db_schema

operations = [[schema, schema_stamp]]

for category in categories[79:]:
    streams = example.get_streams(category, numerated=True, tupled=True)
    load = example.load_into_db
    operations.append([load, schema_stamp, category, streams])

example.db_cascade(operations, dbname='irdb', user='godis_michael', password='211195')