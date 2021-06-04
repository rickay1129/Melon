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

# setup ChromeDriver to perform data crawling
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

# download the top 50 chart provided by Melon
driver = getChromeDriver(headless=False)
driver.get('https://www.melon.com/chart/index.htm')
song_list = driver.find_elements_by_css_selector('#lst50')

# initialize lists to compile data
title_list = list()
artist_list = list()
rank_compile = list()

# for each song, save the title and artist
for song in song_list:
	title = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank01 > span > a').text
	title_list.append(title)
	artist = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank02 > a').text
	artist_list.append(artist)

# go to the next page to download the top 51-100 chart
driver.find_element_by_css_selector('#tb_list > div > span > a').click()
song_list = driver.find_elements_by_css_selector('#lst100')

# for each song, save the title and artist
for song in song_list:
	title = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank01 > span > a').text
	title_list.append(title)
	artist = song.find_element_by_css_selector('td:nth-child(6) > div > div > div.ellipsis.rank02 > a').text
	artist_list.append(artist)

# record the date and time of data collection
today = date.today()
nowT = datetime.now()
time = nowT.strftime("%H_%M_%S")
nowD = today.strftime("%b_%d_%Y")
date = str(nowD).lower()
current_time = date + "_" + time

# each dictionary contains information about rank, title, and artist
for i in range(0,len(title_list)):
	temp = dict()
	# current rank of each song
	temp[current_time] = i + 1
	temp['Title'] = title_list[i]
	temp['Artist'] = artist_list[i]
	# compile dictionaries containing detailed information of all songs 
	rank_compile.append(temp)

# close the WebDriver
driver.close()	

# establish connection to the current database
mydb = mysql.connector.connect(
	host = "127.0.0.1",
	user = "root",
	passwd = "00000000",
	database = 'testdb'
)

# initialize the cursor of the database
mycursor = mydb.cursor()

# compile titles of songs that are stored in the database
mycursor.execute("SELECT Title FROM csv")
myresult = mycursor.fetchall()
# remove irrelevant values
compiled_result = list()
for i in range(0, len(myresult)):
	compiled_result.append(myresult[i][0])

# create a new column for the current date
sql = "ALTER TABLE csv ADD " + str(current_time) + " VARCHAR(100) NOT NULL"
mycursor.execute(sql)

# compile column names in the database
mycursor.execute("SHOW columns FROM csv")
column_list = mycursor.fetchall()
# remove irrelevant values
compiled_column = list()
for i in range(0,len(column_list)):
	compiled_column.append(column_list[i][0])

# update ranking for existing and new songs
for i in range(0,len(title_list)):
	# if the song is already registered, find the corresponding row and update the rank for current date
	if rank_compile[i]['Title'] in compiled_result:
		sql = "UPDATE csv SET " + str(current_time) + "= %s WHERE Title=%s"
		mycursor.execute(sql, (i+1, rank_compile[i]['Title']))
	# if the song is not registered, register its title and artist and insert 0 for the past dates
	elif rank_compile[i]['Title'] not in compiled_result:
		category = ""
		for j in range(0,len(compiled_column)):
			if j != len(compiled_column)-1:
				category += str(compiled_column[j]) + ","
			else:
				category += str(compiled_column[j])
		category_vals = ""
		for j in range(0,len(compiled_column)):
			if j == 0:
				category_vals += "'" + str(rank_compile[i]['Artist']) + "'" + ","
			elif j == 1:
				category_vals += "'" + str(rank_compile[i]['Title']) + "'" + ","
			elif j != len(compiled_column) - 1:
				category_vals += "'" + "0',"
			else:
				category_vals += "'" + str(i+1) + "'"

		sql = "INSERT INTO csv(" + category + ") VALUES (" + category_vals + ")"
		mycursor.execute(sql)

# if there is a song that is no longer included in the top 100 chart, insert 0 for the current date
for i in range(0, len(compiled_column)):
	sql = "UPDATE csv SET " + str(compiled_column[i]) + "=0 WHERE " + str(compiled_column[i]) + " = ''"
	mycursor.execute(sql)

# save the changes to the database
mydb.commit()
