import re

from argparse import ArgumentParser
from argparse import ArgumentTypeError
from argparse import RawTextHelpFormatter
from internetradio import RadioStreamsScrapper


def argparse_range(string):
    values = re.search(r'^(\d+)-(\d+)$', string)
    if values == None:
        msg = "invalid range value: {}".format(string)
        raise ArgumentTypeError(msg)
    elif int(values.groups()[0]) >= int(values.groups()[1]):
        msg = "second value should be bigger than first one"
        raise ArgumentTypeError(msg)
    return values.groups()


def categories_iterate(target, iterable):
    for i in iterable:
        category_streams = target.get_streams(i, numerated=True, tupled=True)
        target.load_into_db(i, category_streams, 'irdb', 'godis_michael', '211195')


def main():
    main_parser = ArgumentParser(description='Internet Radio Streams scrapper for https://www.internet-radio.com/\n',
                                 formatter_class=RawTextHelpFormatter)
    subpasers = main_parser.add_subparsers(dest='command', help='List of commands')
    main_parser.add_argument('-l', '--link', required=False, type=str, default='https://www.internet-radio.com/',
                             help='Link to resource (default: https://www.internet-radio.com/')
    show_parser = subpasers.add_parser('show-genres', help='Show available genres')
    load_parser = subpasers.add_parser('populate-db', help='Load streams into database')
    load_parser.add_argument('-a', '--all', required=False, action='store_true', help='Select all categories')
    load_parser.add_argument('-o', '--one', required=False, type=int, help='Select one category (usage: \'7\')')
    load_parser.add_argument('-r', '--range', required=False, type=argparse_range,
                             help='Select categories range (usage: \'12-35\')')
    load_parser.add_argument('-f', '--few', required=False, type=int, nargs='*',
                             help='Select few categories (usage: \'5 8 11 25\')')
    args = main_parser.parse_args()

    resource = RadioStreamsScrapper(args.link)
    if args.command == 'show-genres':
        categories = resource.load_categories()
        for index, category in enumerate(categories):
            print('%d - %s' % (index, category))

    elif args.command == 'populate-db':
        if not (args.all or args.one or args.range or args.few):
            print('No action requested, add any command')
        else:
            categories = resource.load_categories()
    else:
        print('No commands provided. Try again with any available command.')


if __name__ == '__main__':
    main()
