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

        """ Check url for correct ending. If no backslash provided at the end of url, will be automatically
        added. """

        if self.url[-1] != '/':
            print(
                'Warning! Url provided has no \'/\' sign at the end. It will be added automatically for correct work.')
            self.url += '/'

    @staticmethod
    def _load_page(url):

        """ Load page and return it as a soup object. """

        try:
            open_page = requests.get(url).text

            return bs4.BeautifulSoup(open_page, 'lxml')
        except Exception as e:
            print(e)

    def load_categories(self, tag='div', class_name='panel-body', category_tag='a'):

        """ Return list of site categories available to process. """

        bar = pb.ProgressBar()
        categories = []
        block = self._load_page(self.url).find(tag, attrs={'class': class_name})

        print('Loading streams categories')
        for category in bar(block.find_all(category_tag)):
            categories.append(category.text)
        print('Finished\n')

        return categories

    @staticmethod
    def _get_streams_num(soup, tag='p', _class='lead'):

        """ Number of streams available to download for certain category. """

        paragraph = soup.find(tag, attrs={'class': _class})
        streams_num = re.search(r'\d+', paragraph.text)

        return int(streams_num.group(0))

    @staticmethod
    def _get_pages_num(soup, tag='li', _class='next'):

        """ Number of paginated pages for certain category. """

        try:
            return int(soup.find(tag, attrs={'class': _class}).previous_sibling.text)
        except AttributeError:
            return 1

    def _create_temp_dir(self):

        """ Create temporary directory for downloaded streams. """

        self._remove_temp_dir()
        os.makedirs('tmp')

    @staticmethod
    def _remove_temp_dir():

        """ Remove temporary directory. """

        if os.path.exists('tmp'):
            shutil.rmtree('tmp')

    @staticmethod
    def _numerate_streams(array):

        """ Create additional column in array with streams id`s. """

        rows = array.shape[0]
        numeration = np.arange(1, rows + 1).reshape(-1, 1)
        numerated_array = np.concatenate((numeration, array), axis=1)

        return numerated_array

    @staticmethod
    def _create_tuple(array):

        """ Convert array into tuple (for loading into database). """

        return tuple([tuple(row) for row in array])

    def _download_streams(self, soup, streams=np.empty(shape=(0, 2))):

        """ Download all streams from a page. """

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

        """ Get all streams for certain category. Streams will be numerated and(or) converted to tuple if
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
    def init_db_connection(operations, dbname, user, password, host='localhost'):

        """ Cascade for opening connection with database. To perform operations to database pass a list of operations
        as first parameter.

            For example:
            [[method_name, parametr1, parametr2],
             [method_name2, parametr1, parametr2, parametr3]] """

        con = None

        try:
            con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
            print('Opened database connection successfully\n')
            cur = con.cursor()

            for op in operations:
                func = op[0]
                if len(op) > 1:
                    params = op[1:]
                    func(cur, *params)
                else:
                    func(cur)

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
    def create_schema(cur, name):

        """ Create new schema in database """

        print('Creating new schema in database with \'{}\' label'.format(name))

        cur.execute("SELECT schema_name FROM information_schema.schemata")
        exist_schemas = cur.fetchall()

        drop_schema_query = sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
        create_schema_query = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name))

        if (name,) in exist_schemas:
            cur.execute(drop_schema_query)
            cur.execute(create_schema_query)
        else:
            cur.execute(create_schema_query)

        print('\'{}\' schema has been successfully created'.format(name))

    @staticmethod
    def get_schemas(dbname, user, password, host='localhost'):

        """ Show available database schemas  """

        con = None

        try:
            con = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password))
            cur = con.cursor()

            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN "
                        "('information_schema', 'public') AND schema_name !~ E'^pg_' ")
            exist_schemas = cur.fetchall()
            exist_schemas = [schema[0] for schema in exist_schemas]

            return exist_schemas

        except psycopg2.DatabaseError as e:
            if con:
                con.rollback()

            print('Error %s' % e)
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def load_into_db(cur, schema_name, table_name, data):

        """ Load data into a database  """

        print('Loading category \'{}\' into database...'.format(table_name))

        drop_query = sql.SQL("DROP TABLE IF EXISTS {}")\
            .format(sql.SQL('.').join([sql.Identifier(schema_name), sql.Identifier(table_name)]))
        create_query = sql.SQL("CREATE TABLE {}(Id INT PRIMARY KEY, Name TEXT, Link text)")\
            .format(sql.SQL('.').join([sql.Identifier(schema_name), sql.Identifier(table_name)]))
        insert_query = sql.SQL("INSERT INTO {}(id, Name, Link) VALUES (%s, %s, %s)")\
            .format(sql.SQL('.').join([sql.Identifier(schema_name), sql.Identifier(table_name)]))

        cur.execute(drop_query)
        cur.execute(create_query)
        cur.executemany(insert_query, data)

        print('Finished loading')

    @staticmethod
    def _show_tables(cur, schema_name):

        """ List of available tables for certain schema """

        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
        cur.execute(query, (schema_name,))
        exist_tables = cur.fetchall()
        exist_tables = [table[0] for table in exist_tables]

        return exist_tables

    def update_db(self, cur, schema, dest='public'):

        """ 1. Add new tables (if don`t exist) to result(public) schema from other stored schema
            2. Add new streams to result schema if they don`t exist
            3. Rename stream in the result schema if name differs from one in stored schema """

        schema_tables = self._show_tables(cur, schema)
        public_tables = self._show_tables(cur, dest)

        bar = pb.ProgressBar()
        print('Making update from \'{}\' schema...'.format(schema))
        for table in bar(schema_tables):
            if table in public_tables:
                cur.execute(sql.SQL("SELECT * FROM {}").format(sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])))
                schema_data = cur.fetchall()
                schema_data = {row[2]:row[1] for row in schema_data}

                cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
                public_data = cur.fetchall()
                index = max([item[0] for item in public_data]) + 1
                public_data = {row[2]:row[1] for row in public_data}

                for link,name in schema_data.items():
                    if link in public_data and name != public_data[link]:
                        query = sql.SQL("UPDATE {} SET name=%s WHERE link=%s").format(sql.Identifier(table))
                        cur.execute(query, (name, link))
                    elif link not in public_data:
                        query = sql.SQL("INSERT INTO {}(id, Name, Link) VALUES (%s, %s, %s)").format(sql.Identifier(table))
                        data = (index, name, link)
                        # print(type(index))
                        cur.execute(query, data)
                        index += 1
