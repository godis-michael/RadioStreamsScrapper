import re

from getpass import getpass
from datetime import datetime
from argparse import ArgumentParser
from argparse import ArgumentTypeError
from iradio import RadioStreamsScrapper
from argparse import RawTextHelpFormatter


def argparse_range(string):

    """ Definition of range type for argument parser. Example of value: '23-47'. """

    values = re.search(r'^(\d+)-(\d+)$', string)
    if values is None:
        msg = "invalid range value: {}".format(string)
        raise ArgumentTypeError(msg)
    elif int(values.groups()[0]) >= int(values.groups()[1]):
        msg = "second value should be bigger than first one"
        raise ArgumentTypeError(msg)
    return values.groups()


def add_load_operation(target, category, schema_name, operations=None):

    """ Load streams and add operation to be performed on database. """

    operations = operations or []
    streams = target.get_streams(category, numerated=True, tupled=True)
    operations.append([target.load_into_db, schema_name, category, streams])

    return operations


def db_params():

    """ Ask for data to connect with database and return them. """

    print('Establishing connection with database...')

    dbname = input('Enter database name: ')
    user = input('user: ')
    password = getpass('password: ')

    return {'dbname': dbname, 'user': user, 'password': password}


def main():
    # ===================================
    # =========PARSING ARGUMENTS=========
    # ===================================
    main_parser = ArgumentParser(description='Internet Radio Streams scrapper for https://www.internet-radio.com/\n',
                                 formatter_class=RawTextHelpFormatter)
    subpasers = main_parser.add_subparsers(dest='command', help='List of commands')
    main_parser.add_argument('-l', '--link', required=False, type=str, default='https://www.internet-radio.com/',
                             help='Link to resource (default: https://www.internet-radio.com/')
    genres_parser = subpasers.add_parser('genres', help='Show available genres')
    schemas_parser = subpasers.add_parser('schemas', help='Show available schemas')
    populate_parser = subpasers.add_parser('populate', help='Load streams into database')
    populate_parser.add_argument('-a', '--all', required=False, action='store_true', help='Select all categories')
    populate_parser.add_argument('-o', '--one', required=False, type=int, help='Select one category (usage: \'7\')')
    populate_parser.add_argument('-r', '--range', required=False, type=argparse_range,
                                 help='Select categories range (usage: \'12-35\')')
    populate_parser.add_argument('-f', '--few', required=False, type=int, nargs='*',
                                 help='Select few categories (usage: \'5 8 11 25\')')
    update_parser = subpasers.add_parser('update', help='Update public schema from available schemas')
    update_parser.add_argument('-a', '--all', required=False, action='store_true', help='Select all schemas')
    update_parser.add_argument('-o', '--one', required=False, type=int, help='Select one schema (usage: \'3\')')
    update_parser.add_argument('-r', '--range', required=False, type=argparse_range,
                               help='Select schemas range (usage: \'2-5\')')
    update_parser.add_argument('-f', '--few', required=False, type=int, nargs='*',
                               help='Select few schemas (usage: \'1 3 6\')')
    args = main_parser.parse_args()
    # ===================================
    # =============END BLOCK=============
    # ===================================

    resource = RadioStreamsScrapper(args.link)  # Initialize scrapper

    # ===================================
    # ==========AVAILABLE GENRES=========
    # ===================================
    if args.command == 'genres':
        categories = resource.load_categories()
        for index, category in enumerate(categories):
            print('%d - %s' % (index, category))
    # ===================================
    # =============END BLOCK=============
    # ===================================

    # ===================================
    # =========AVAILABLE SCHEMAS=========
    # ===================================
    elif args.command == 'schemas':
        conn_params = db_params()
        schemas = resource.get_schemas(**conn_params)

        print('Available schemas:')
        for index,schema in enumerate(schemas):
            print('%d - %s' % (index, schema))
    # ===================================
    # =============END BLOCK=============
    # ===================================

    # ===================================
    # ==========DOWNLOAD STREAMS=========
    # ===================================
    elif args.command == 'populate':
        if not (args.all or args.one or args.range or args.few):
            print('No action requested, add avaliable command')
        else:
            conn_params = db_params()
            categories = resource.load_categories()  # Load available categories
            timestamp = '{:%Y-%b-%d %H:%M}'.format(datetime.now())
            operations = [[resource.create_schema, timestamp]]

            if args.all:
                for c in categories:
                    operations += add_load_operation(resource, c, timestamp)
            elif args.one:
                operations += add_load_operation(resource, categories[args.one], timestamp)
            elif args.range:
                for c in categories[int(args.range[0]):int(args.range[1]) + 1]:
                    operations += add_load_operation(resource, c, timestamp)
            elif args.few:
                for _ in args.few:
                    operations += add_load_operation(resource, categories[_], timestamp)
            resource.init_db_connection(operations, **conn_params)
    # ===================================
    # =============END BLOCK=============
    # ===================================

    # ===================================
    # ============UPDATE MAIN============
    # ===================================
    elif args.command == 'update':
        if not (args.all or args.one or args.range or args.few):
            print('No action requested, add avaliable command')
        else:
            conn_params = db_params()
            schemas = resource.get_schemas(**conn_params)
            operations = []

            if args.all:
                for s in schemas:
                    operations.append([resource.update_db, s])
            elif args.one:
                operations.append([resource.update_db, schemas[args.one]])
            elif args.range:
                for s in schemas[int(args.range[0]):int(args.range[1]) + 1]:
                    operations.append([resource.update_db, s])
            elif args.few:
                for _ in args.few:
                    operations.append([resource.update_db, schemas[_]])
            resource.init_db_connection(operations, **conn_params)
    # ===================================
    # =============END BLOCK=============
    # ===================================
    else:
        print('No commands provided. Try again with any available command.')

if __name__ == '__main__':
    main()
