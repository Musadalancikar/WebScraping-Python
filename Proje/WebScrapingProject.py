from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymongo
from pymongo import MongoClient
import re

def scrape_kitapyurdu():
    # Kitapyurdu web sitesinden veri kazıma işlemi yapar ve DataFrame döndürür.
    r = requests.get('https://www.kitapyurdu.com/index.php?route=product/best_sellers&list_id=16&filter_in_stock=1')
    source = BeautifulSoup(r.content, "lxml")

    book_name = source.find_all("div", attrs={"class":"product-cr"})
    publisher_name = source.find_all("div", attrs={"class":"publisher"})
    author_name = source.find_all("div", attrs={"class":"author"})
    prices = source.find_all("div", attrs={"class": "price"})

    book_title = []
    publisher_name_list = []
    author_name_list = []
    price_list = []

    # Kitap adları, yayınevi, yazar ve fiyat bilgilerini alır.
    for i in book_name:
        x_name_ellipsis = i.find("div", attrs={"class":"name ellipsis"})
        y_title = x_name_ellipsis.find("a").get("title")
        book_title.append(y_title)

    for i in publisher_name:
        x_publisher = i.find("a")
        y_publisher_name = x_publisher.find("span").text
        publisher_name_list.append(y_publisher_name)

    for i in author_name:
        x_author_name = i.find("a")
        y_author_name = x_author_name.find("span")
        if y_author_name and isinstance(y_author_name, type(y_author_name)):
            a = y_author_name.text
            author_name_list.append(a.strip())

    for i in prices:
        x_prices = i.find("div", attrs={"class":"price-new"})
        y_prices = x_prices.find("span", attrs={"class":"value"})
        if y_prices is not None:
            price_text = y_prices.text.strip()
            numeric_price = re.sub(r'[^\d.,]+', '', price_text)
            price_list.append(numeric_price)

    # Kitapyurdu verilerini içeren DataFrame'i oluşturur ve döndürür.
    ky_df = pd.DataFrame({'Book Name': book_title, 'Book Publisher': publisher_name_list, 'Book Author': author_name_list, 'price': price_list})
    return ky_df

def scrape_kitapsepeti():
    # Kitapsepeti web sitesinden veri kazıma işlemi yapar ve DataFrame döndürür.
    r1 = requests.get('https://www.kitapsepeti.com/cok-satan-kitaplar')
    source1 = BeautifulSoup(r1.content, "lxml")

    ks_book = source1.find_all("div", attrs={"class":"col col-3 col-md-4 col-sm-6 col-xs-6 p-right mb productItem zoom ease"})

    ks_book_title = []
    ks_publisher_name_list = []
    ks_author_name_list = []
    ks_price_list = []

    # Kitap adları, yayınevi, yazar ve fiyat bilgilerini alır.
    for i in ks_book:
        ks_x_title = i.find("div", attrs={"class":"box col-12 text-center"})
        ks_y_title = ks_x_title.find("a")
        ks_book_title.append(ks_y_title.text.strip())

    for i in ks_book:
        ks_x_publisher = i.find("div", attrs={"class":"box col-12 text-center"})
        ks_y_publisher = ks_x_publisher.find_all("a")
        ks_z_publisher = ks_y_publisher[1]
        ks_publisher_name_list.append(ks_z_publisher.text)

    for i in ks_book:
        ks_x_author = i.find("div", attrs={"class":"box col-12 text-center"})
        ks_y_author = ks_x_author.find_all("a")
        ks_z_author = ks_y_author[2]
        ks_author_name_list.append(ks_z_author.text)

    for i in ks_book:
        ks_x_prices = i.find("div", attrs={"class":"box col-10 col-ml-1 col-sm-12 proRowAct"})
        ks_y_prices = ks_x_prices.find("div", attrs={"class":"col col-12 currentPrice"})
        a = ks_y_prices.text.strip()
        b = a.replace("\n","")
        c = b.strip("TL")
        ks_price_list.append(c)

    # Kitapsepeti verilerini içeren DataFrame'i oluşturur ve döndürür.
    ks_df = pd.DataFrame({'Book Name': ks_book_title, 'Book Publisher': ks_publisher_name_list, 'Book Author': ks_author_name_list, 'price': ks_price_list})
    return ks_df

def save_to_mongodb(df, collection_name):
    # Verilen DataFrame'i MongoDB'ye kaydeder.
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = myclient["sm"]
    collection = mydb[collection_name]
    documents = df.to_dict(orient='records')
    collection.insert_many(documents)

# Kitapyurdu verilerini kazıyarak DataFrame oluşturur.
ky_df = scrape_kitapyurdu()

# Kitapsepeti verilerini kazıyarak DataFrame oluşturur.
ks_df = scrape_kitapsepeti()

# Kitapyurdu DataFrame'i MongoDB'ye kaydeder.
save_to_mongodb(ky_df, "kitapyurdu")

# Kitapsepeti DataFrame'i MongoDB'ye kaydeder.
save_to_mongodb(ks_df, "kitapsepeti")
