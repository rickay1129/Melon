from datetime import datetime, date
import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from pandas import DataFrame
import mysql.connector
from sqlalchemy import create_engine

import pymysql
pymysql.install_as_MySQLdb()

def getChromeDriver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.implicitly_wait(2)
    return driver

# download the top 50 chart
driver = getChromeDriver(headless=False)
driver.get('https://www.melon.com/chart/index.htm')
song_list = driver.find_elements_by_css_selector('#lst50')

title_list = list()
artist_list = list()

# for each song, save the title, artist, and number of likes
for song in song_list:
	title = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank01 > span > a').text
	title_list.append(title)
	artist = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank02 > a').text
	artist_list.append(artist)

driver.find_element_by_css_selector('#tb_list > div > span > a').click()
song_list = driver.find_elements_by_css_selector('#lst100')
# top 51-100 list
for song in song_list:
	title = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank01 > span > a').text
	title_list.append(title)
	artist = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank02 > a').text
	artist_list.append(artist)

today = date.today()
nowT = datetime.now()
time = nowT.strftime("%H_%M_%S")
nowD = today.strftime("%b_%d_%Y")
date = str(nowD).lower()

current_time = date + "_" + time

rank_compile = list()

for i in range(0,len(title_list)):
	temp = dict()
	temp[current_time] = i + 1
	temp['Title'] = title_list[i]
	temp['Artist'] = artist_list[i]

	rank_compile.append(temp)

df = pd.DataFrame(rank_compile)
address = os.path.abspath('melon.py')
pathway = os.path.dirname(address)
df.to_excel(pathway + '/test_data.xlsx',index=False)

read_file = pd.read_excel(pathway + '/test_data.xlsx')
read_file.to_csv(pathway + '/test_data.csv', index=False)

# rank is just based on the order of title_list
driver.close()

# connect to DB and create new table with CSV data
engine = create_engine('mysql://root:cary11292083@127.0.0.1:3306/testdb')
df = pd.read_csv(pathway + '/test_data.csv', header=0)
with engine.connect() as conn, conn.begin():
	df.to_sql('csv', conn, if_exists='replace', index=False)

