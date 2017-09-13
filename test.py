import psycopg2
from psycopg2 import sql
import sys
from internetradio import RadioStreamsScrapper

example = RadioStreamsScrapper('https://www.internet-radio.com/')
categories = example.load_categories()

example.update_db('rock', 123, 'irdb', 'godis_michael', '211195')