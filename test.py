import urllib.request
import requests
import bs4
import os
import shutil
import numpy as np


class RadioStreamsScrapper:
    def __init__(self, url):
        self.url = url

    @staticmethod
    def load_page(url):
        open_page = requests.get(url).text

        return bs4.BeautifulSoup(open_page, 'lxml')

    def load_categories(self, block_tag, class_name, category_tag):
        categories = []
        block = self.load_page(self.url).find(block_tag, attrs={'class': class_name})

        for category in block.find_all(category_tag):
            categories.append(category.text)

        return categories, block

    @staticmethod
    def add_featured(categories, soup, block_tag, class_name, category_tag):
        block = soup.find_next(block_tag, attrs={'class': class_name})

        for featured in block.find_all(category_tag):
            categories.append(featured.text[1:])

        return categories

    @staticmethod
    def get_pages(soup, pagination_tag='li', pagination_class='next'):
        try:
            return soup.find(pagination_tag, attrs={'class': pagination_class}).previous_sibling.text
        except AttributeError:
            return 0

    @staticmethod
    def create_temp_dir(name='tmps'):
        if os.path.exists(name):
            shutil.rmtree(name)
            os.makedirs(name)
        else:
            os.makedirs(name)

    def download(self, soup, curr_page_streams=np.array([])):
        self.create_temp_dir()
        # initial_soup = self.load_page(url)
        stream_rows = soup.find_all('tr')
        index = 1
        for stream in stream_rows:
            stream_name = stream.find('h4').text
            stream_link = stream.find('a', title='M3U Playlist File')['href']
            urllib.request.urlretrieve(self.url + stream_link[1:], 'tmps/temp-'+str(index))
            with open('tmps/temp-' + str(index), 'r+') as f:
                stream_link = f.readline()[:-1]
            curr_page_streams = np.append(curr_page_streams, [stream_name, stream_link], axis=0)
            index += 1
        return curr_page_streams

    def get_streams(self, categories):
        for category in categories:
            category_url = self.url + 'stations/' + 'chill' + '/'
            soup = self.load_page(category_url)
            num_pages = self.get_pages(soup)
            curr_genre_streams = self.download(soup)
            if num_pages:
                for page in range(2,int(num_pages)):
                    next_page_url = category_url + 'page' + str(page)
                    soup = self.load_page(next_page_url)
                    curr_genre_streams = self.download(soup, curr_genre_streams)
            return curr_genre_streams


example = RadioStreamsScrapper('https://www.internet-radio.com/')
page = example.load_page('https://www.internet-radio.com/')
categories, soup = example.load_categories('div', 'panel-body', 'a')
# categories_and_featured = example.add_featured(categories, soup, 'div', 'row', 'h2')
for _ in example.get_streams(categories):
    print(_)

print(example.get_streams(categories).shape)






#
# internet_radio_url = 'https://www.internet-radio.com/'
#
# open_page = requests.get(internet_radio_url).text
# soup = bs4.BeautifulSoup(open_page, 'lxml')
#
# categories_div = soup.find('div', attrs={'class': 'panel-body'})
#
# categories = []
#
# for category in categories_div.find_all('a'):
#     categories.append(category.text)
#
# featured_div = categories_div.find_next('div', attrs={'class': 'row'})
#
# for featured in featured_div.find_all('h2'):
#     categories.append(featured.text[1:])
#
# for category in categories[:-2]:
#     category_url = internet_radio_url + 'stations/' + category + '/'
#     open_category = requests.get(category_url).text
#     category_soup = bs4.BeautifulSoup(open_category, 'lxml')
#
#     m3u = category_soup.find_all('a', title='M3U Playlist File')
#     for stream in m3u:
#         print(stream)
#         break
#     break

# urllib.request.urlretrieve("https://www.internet-radio.com/servers/tools/playlistgenerator/?u=http://us2.internet-radio.com:8046/listen.pls&t=.m3u", "/home/jun/Downloads/test.m3u")
