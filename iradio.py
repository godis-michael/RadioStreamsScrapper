import os
import re
import sys
import bs4
import shutil
import requests
import psycopg2
import urllib.request

import numpy as np
import progressbar as pb

from psycopg2 import sql


class RadioStreamsScrapper:

    """ Radio Streams Scrapper v0.1 for https://www.internet-radio.com/ """

    def __init__(self, url):
        self.url = url
        self.__check_url()

    def __check_url(self):

        """ Checks url for correct ending. If no backslash provided at the end of url, it will be automatically
        added. """

        if self.url[-1] != '/':
            print(
                'Warning! Url provided has no \'/\' sign at the end. It will be added automatically for correct work.')
            self.url += '/'

    @staticmethod
    def _load_page(url):

        """ Loads page and returns it as a soup object. """

        try:
            open_page = requests.get(url).text

            return bs4.BeautifulSoup(open_page, 'lxml')
        except Exception as e:
            print(e)

    def load_categories(self, tag='div', class_name='panel-body', category_tag='a'):

        """ Returns list of site categories available to process. """

        bar = pb.ProgressBar()
        categories = []
        block = self._load_page(self.url).find(tag, attrs={'class': class_name})

        print('Loading streams categories')
        for category in bar(block.find_all(category_tag)):
            categories.append(category.text)
        print('Finished\n')

        return categories

    @staticmethod
    def _get_streams_num(soup, tag='p', class_name='lead'):

        """ Number of streams available to download for certain category. """

        paragraph = soup.find(tag, attrs={'class': class_name})
        streams_num = re.search(r'\d+', paragraph.text)

        return int(streams_num.group(0))

    @staticmethod
    def _get_pages_num(soup, tag='li', class_name='next'):

        """ Number of pages for certain category. """

        try:
            return int(soup.find(tag, attrs={'class': class_name}).previous_sibling.text)
        except AttributeError:
            return 1

    def _create_temp_dir(self):

        """ Creates temporary directory for downloaded streams. """

        self._remove_temp_dir()
        os.makedirs('tmp')

    @staticmethod
    def _remove_temp_dir():

        """ Removes temporary directory. """

        if os.path.exists('tmp'):
            shutil.rmtree('tmp')

    @staticmethod
    def _numerate_streams(array):

        """ Creates additional column in array with streams id`s. """

        rows = array.shape[0]
        numeration = np.arange(1, rows + 1).reshape(-1, 1)
        numerated_array = np.concatenate((numeration, array), axis=1)

        return numerated_array

    @staticmethod
    def _create_tuple(array):

        """ Converts array into tuple (for loading into database). """

        return tuple([tuple(row) for row in array])

    def _download_streams(self, soup, streams=np.empty(shape=(0, 2))):

        """ Downloads all streams from a page. """

        self._create_temp_dir()
        index = 1
        stream_rows = soup.find_all('tr')

        print('\nDownloading streams')
        for stream in stream_rows:
            stream_name = stream.find('h4').text
            stream_link = stream.find('a', title='M3U Playlist File')['href']

            try:
                urllib.request.urlretrieve(self.url + stream_link[1:], 'tmp/temp-' + str(index))
                with open('tmp/temp-' + str(index), 'r+') as f:
                    stream_link = f.readline()[:-1]
                streams = np.append(streams, [[stream_name, stream_link]], axis=0)
                index += 1
            except Exception as e:
                print('Exception found:', stream_name, stream_link, e)
        self._remove_temp_dir()
        print('Finished')

        return streams

    def get_streams(self, category, numerated=False, tupled=False):

        """ Gets all streams for certain category. Streams will be numerated and(or) converted to tuple if
        'numerated'/'tupled' is set to True. """

        print('\nStarting download for category "%s"' % category)
        category_url = self.url + 'stations/' + category + '/'
        soup = self._load_page(category_url)
        num_streams = self._get_streams_num(soup)
        num_pages = self._get_pages_num(soup)

        print('%d page(s) and %d streams detected' % (num_pages, num_streams))
        streams = self._download_streams(soup)

        if num_pages != 1:
            print('\nStartind download for other %d page(s) of this category' % (int(num_pages) - 1))
            page_bar = pb.ProgressBar()
            for page in page_bar(range(2, int(num_pages) + 1)):
                next_page_url = category_url + 'page' + str(page)
                soup = self._load_page(next_page_url)
                streams = self._download_streams(soup, streams)
        print('Finished category download\n')

        if numerated:
            streams = self._numerate_streams(streams)
        if tupled:
            streams = self._create_tuple(streams)

        return streams

    @staticmethod
    def db_cascade(operations, dbname, user, password, host='localhost'):
        con = None

        try:
            con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
            print('Opened database successfully')
            cur = con.cursor()

            for op in operations:
                if len(op) == 2:
                    func = op[0]
                    arg = op[1]
                    func(con, cur, arg)
                elif len(op) == 4:
                    func = op[0]
                    arg1,arg2,arg3 = op[1:]
                    func(con, cur, arg1, arg2, arg3)

            con.commit()

        except psycopg2.DatabaseError as e:
            if con:
                con.rollback()

            print('Error %s' % e)
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def create_db_schema(con, cur, schema_name):
        print('Creating new schema in database with \'{}\' label'.format(schema_name))

        cur.execute("SELECT schema_name FROM information_schema.schemata")
        exist_schemas = cur.fetchall()

        drop_schema_query = sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
        create_schema_query = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))

        if (schema_name,) in exist_schemas:
            cur.execute(drop_schema_query)
            cur.execute(create_schema_query)
        else:
            cur.execute(create_schema_query)

        print('\'{}\' schema has been successfully created'.format(schema_name))

    @staticmethod
    def load_into_db(con, cur, schema_name, table_name, data):
        print('Loading category \'{}\' into database...'.format(table_name))

        dest = '.'.join([schema_name, table_name])
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(dest))
        create_query = sql.SQL("CREATE TABLE {}(Id INT PRIMARY KEY, Name TEXT, Link text)").format(
            sql.Identifier(dest))
        insert_query = sql.SQL("INSERT INTO {} (id, Name, Link) VALUES (%s, %s, %s)").format(
            sql.Identifier(dest))

        cur.execute(drop_query)
        cur.execute(create_query)
        cur.executemany(insert_query, data)

        print('Finished loading')

    # @staticmethod
    # def create_db_schema(dbname, user, password, host='localhost'):
    #     name = '{:%Y-%b-%d %H:%M}'.format(datetime.now())
    #     con = None
    #
    #     try:
    #         print('Creating new image for data with \'{}\' label...'.format(name))
    #         con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
    #         print('Opened database successfully')
    #         cur = con.cursor()
    #
    #         cur.execute("SELECT schema_name FROM information_schema.schemata")
    #         exist_schemas = cur.fetchall()
    #
    #         drop_schema_query = sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
    #         create_schema_query = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name))
    #
    #         if (name,) in exist_schemas:
    #             cur.execute(drop_schema_query)
    #             cur.execute(create_schema_query)
    #         else:
    #             cur.execute(create_schema_query)
    #
    #         con.commit()
    #         # print('Finished loading')
    #
    #     except psycopg2.DatabaseError as e:
    #         if con:
    #             con.rollback()
    #
    #         print('Error %s' % e)
    #         sys.exit(1)
    #
    #     finally:
    #         if con:
    #             con.close()

    # @staticmethod
    # def load_into_db(table_name, data, dbname, user, password, host='localhost'):
    #
    #     """ Loads data provided into a database. If table for category is already exist it will be overridden. """
    #
    #     con = None
    #
    #     try:
    #         print('Loading category \'{}\' into database...'.format(table_name))
    #         con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
    #         print('Opened database successfully')
    #         cur = con.cursor()
    #
    #         drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
    #         create_query = sql.SQL("CREATE TABLE {}(Id INT PRIMARY KEY, Name TEXT, Link text)").format(
    #             sql.Identifier(table_name))
    #         insert_query = sql.SQL("INSERT INTO {} (id, Name, Link) VALUES (%s, %s, %s)").format(
    #             sql.Identifier(table_name))
    #
    #         cur.execute(drop_query)
    #         cur.execute(create_query)
    #         cur.executemany(insert_query, data)
    #
    #         con.commit()
    #         print('Finished loading')
    #
    #     except psycopg2.DatabaseError as e:
    #         if con:
    #             con.rollback()
    #
    #         print('Error %s' % e)
    #         sys.exit(1)
    #
    #     finally:
    #         if con:
    #             con.close()

    # @staticmethod
    # def update_db(table_name, data, dbname, user, password, host='localhost'):
    #
    #     """ Compares category data with existing table. If category does not exist among tables, it will be
    #     downloaded. """
    #
    #     con = None
    #
    #     try:
    #         print('Updating category \'{}\'...'.format(table_name))
    #         con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
    #         print('Opened database successfully')
    #         cur = con.cursor()
    #
    #         cur.execute("SELECT table_name FROM information_schema.tables "
    #                     "WHERE table_type = 'BASE TABLE' AND table_schema = 'public' "
    #                     "ORDER BY table_type, table_name")
    #         exist_tables = cur.fetchall()
    #
    #         if (table_name,) in exist_tables:
    #             select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    #             cur.execute(select_query)
    #             exist_data = cur.fetchall()
    #             cache = set(itertools.chain.from_iterable(exist_data))
    #             num_objects = exist_data[-1][0]
    #
    #             print(cache)
    #             # for row in data:
    #             #     if row[1] in cache
    #
    #         else:
    #             print('Category \'{}\' does not exist in database. Starting load...')
    #             create_query = sql.SQL("CREATE TABLE {}(Id INT PRIMARY KEY, Name TEXT, Link text)").format(
    #                 sql.Identifier(table_name))
    #             insert_query = sql.SQL("INSERT INTO {} (id, Name, Link) VALUES (%s, %s, %s)").format(
    #                 sql.Identifier(table_name))
    #
    #             cur.execute(create_query)
    #             cur.executemany(insert_query, data)
    #
    #             con.commit()
    #             print('Finished loading')
    #
    #     except psycopg2.DatabaseError as e:
    #         if con:
    #             con.rollback()
    #
    #         print('Error %s' % e)
    #         sys.exit(1)
    #
    #     finally:
    #         if con:
    #             con.close()
