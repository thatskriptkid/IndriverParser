import requests
import json
import urllib2
import sqlite3
import time
import signal
import sys
import socket
import os
import mysql.connector
from datetime import datetime
from mysql.connector import errorcode
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("user", type=str, help="database username")
parser.add_argument("passwd", type=str, help="database password")
args = parser.parse_args()

def change_city(new_cid):
	
	body = {'phone':'+7xxxxxxxx', 'token':'xxxxxxxxxx',
		'v':'4','stream_id':'1540382596387750', 'city_id':new_cid}

	r = requests.post('http://indriver.ru/api/profileedit?cid=150&locale=ru', data = body, 
			headers={'Content-Type':'application/x-www-form-urlencoded',
			'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:22.0) Gecko/20100101 Firefox/22.0'})

def signal_handler(signal, frame):
	conn.commit()
	cursor.close()
	conn.close()
	sys.exit(0)

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

DB_NAME = 'Indriver'

conn = mysql.connector.connect(user=args.user,password=args.passwd, auth_plugin='mysql_native_password', host ='127.0.0.1', )

cursor = conn.cursor(buffered=True)

try:
    cursor.execute("USE {}".format(DB_NAME))
    print('Database exists!')
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        conn.database = DB_NAME
    else:
        print(err)
        exit(1)



table = (
    "CREATE TABLE `Main` ("
    "  `id` MEDIUMINT NOT NULL AUTO_INCREMENT,"
    "  `firstname` VARCHAR(50) NULL,"
    "  `lastname` varchar(50) NULL,"
    "  `birthday` date NULL,"
    "  `carname` varchar(50) NULL,"
    "  `carmodel` varchar(50) NULL,"
    "  `carcolor` varchar(50) NULL,"
    "  `cargosnomer` varchar(50) NULL,"
    "  `caryear` MEDIUMINT NULL,"
    "  `city` varchar(50) NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")


try:
	print("Creating table Main...\n")
	cursor.execute(table)
except mysql.connector.Error as err:
	if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
		print("table Main already exist")
	else:
		print(err.msg)

signal.signal(signal.SIGINT, signal_handler)

body = {'phone':'+7xxxxxxxxx', 'token':'xxxxxxxxxxxxxxxxxxxxxxxxxx',
'v':'2','stream_id':'1540382596387750','source':'map'}

url_params = {'locale':'ru'}

step = 0.01

coords = {
		'Astana':{'lon_min' : 71.39, 'lon_max' : 71.50, 'lat_min' : 51.12, 'lat_max' : 51.17, 'cid':150}, 
		'Almaty':{'lon_min' : 76.86, 'lon_max' : 76.95, 'lat_min' : 43.22, 'lat_max' : 43.25, 'cid' : 169},
		'Kokshetau':{'lon_min' : 69.35, 'lon_max' : 69.40, 'lat_min' : 53.26, 'lat_max' : 53.28, 'cid' : 151},
		'Kostanay':{'lon_min' : 63.60, 'lon_max' : 63.64, 'lat_min' : 53.19, 'lat_max' : 53.22, 'cid' : 252},
		'Aktobe':{'lon_min' : 57.16, 'lon_max' : 57.25, 'lat_min' : 50.25, 'lat_max' : 50.30,'cid' : 213},
		'Atyrau':{'lon_min' : 51.88, 'lon_max' : 51.94, 'lat_min' : 47.10, 'lat_max' : 47.13, 'cid' : 221},
		'Aktau':{'lon_min' : 51.12, 'lon_max' : 51.20, 'lat_min' : 43.64, 'lat_max' : 43.68, 'cid' : 258},
		'Kyzylorda': {'lon_min' : 65.44, 'lon_max' : 65.56, 'lat_min' : 44.79, 'lat_max' : 44.88, 'cid' : 257},
		'Taraz': {'lon_min' : 71.32, 'lon_max' : 71.40, 'lat_min' : 42.85, 'lat_max' : 42.91, 'cid' : 235},
		'Shymkent': {'lon_min' : 69.53, 'lon_max' : 69.65, 'lat_min' : 42.29, 'lat_max' : 42.35, 'cid' : 277},
		'Taldykorgan': {'lon_min' : 78.32, 'lon_max' : 78.41, 'lat_min' : 44.98, 'lat_max' : 45.10, 'cid' : 177},
		'Karaganda': {'lon_min' : 73.06, 'lon_max' : 73.15, 'lat_min' : 49.77, 'lat_max' : 49.82, 'cid' : 242},
		'Pavlodar': {'lon_min' : 76.93, 'lon_max' : 76.99, 'lat_min' : 52.25, 'lat_max' : 52.30, 'cid' : 262},
		'Petropavl': {'lon_min' : 69.10, 'lon_max' : 69.16, 'lat_min' : 54.85, 'lat_max' : 54.88, 'cid' : 266},
		'Semei': {'lon_min' : 80.21, 'lon_max' : 80.28, 'lat_min' : 50.40, 'lat_max' : 50.44, 'cid' : 228},
		'Ustkaman': {'lon_min' : 82.56, 'lon_max' : 82.64, 'lat_min' : 49.94, 'lat_max' : 49.97, 'cid' : 230},
		'Tashkent': {'lon_min' : 69.20, 'lon_max' : 69.35, 'lat_min' : 41.27, 'lat_max' : 41.34, 'cid' : 638},
		'Bishkek': {'lon_min' : 74.51, 'lon_max' : 74.65, 'lat_min' : 42.83, 'lat_max' : 42.89, 'cid' : 639},
		'Omsk': {'lon_min' : 73.21, 'lon_max' : 73.51, 'lat_min' : 54.90, 'lat_max' : 55.04, 'cid' : 578},
		'Barnaul': {'lon_min' : 83.66, 'lon_max' : 83.80, 'lat_min' : 53.33, 'lat_max' : 53.40, 'cid' : 279},
		'Tomsk': {'lon_min' : 84.92, 'lon_max' : 84.92, 'lat_min' : 56.46, 'lat_max' : 56.54, 'cid' : 12},
		'Yakutsk': {'lon_min' : 129.70, 'lon_max' : 129.75, 'lat_min' : 62.00, 'lat_max' : 62.04, 'cid' : 1},
		'Ekibastuz': {'lon_min' : 75.28, 'lon_max' : 75.35, 'lat_min' : 51.69, 'lat_max' : 51.74, 'cid' : 263},
		'Balhash': {'lon_min' : 74.95, 'lon_max' : 75.00, 'lat_min' : 46.83, 'lat_max' : 46.85, 'cid' : 240},
		'Temirtau': {'lon_min' : 72.92, 'lon_max' : 73.00, 'lat_min' : 50.04, 'lat_max' : 50.06, 'cid' : 248},
		'Pyatigorsk': {'lon_min' : 43.11, 'lon_max' : 42.99, 'lat_min' : 44.00, 'lat_max' : 44.05, 'cid' : 742},
		'Vladivostok': {'lon_min' : 131.87, 'lon_max' : 131.95, 'lat_min' : 43.11, 'lat_max' : 43.14, 'cid' : 11},
		'Krasnoyarsk': {'lon_min' : 92.80, 'lon_max' : 92.96, 'lat_min' : 55.98, 'lat_max' : 56.02, 'cid' : 182},
		'Surgut': {'lon_min' : 73.35, 'lon_max' : 73.44, 'lat_min' : 61.23, 'lat_max' : 61.27, 'cid' : 87},
		'Tumen': {'lon_min' : 65.47, 'lon_max' : 65.60, 'lat_min' : 57.11, 'lat_max' : 57.17, 'cid' : 146},
		'Perm': {'lon_min' : 57.95, 'lon_max' : 58.00, 'lat_min' : 56.12, 'lat_max' : 56.30, 'cid' : 110},
		'Habarovsk': {'lon_min' : 135.03, 'lon_max' : 135.13, 'lat_min' : 48.45, 'lat_max' : 48.51, 'cid' : 79},
		'Ulan-Ude': {'lon_min' : 107.53, 'lon_max' : 107.70, 'lat_min' : 51.80, 'lat_max' : 51.85, 'cid' : 10}
		}

#33 cities
cities = ['Omsk','Bishkek', 'Tashkent','Astana', 'Almaty','Kokshetau','Kostanay','Aktobe','Atyrau','Aktau',
	'Kyzylorda', 'Taraz', 'Shymkent', 'Taldykorgan', 'Karaganda', 'Pavlodar',
	'Petropavl','Semei','Ustkaman', 'Barnaul', 'Tomsk','Yakutsk', 'Ekibastuz','Balhash','Temirtau','Pyatigorsk','Vladivostok',
	'Krasnoyarsk','Surgut','Tumen', 'Perm','Habarovsk','Ulan-Ude']

while True:
	for city in cities:

		# set current city 
		new_cid = coords[city]['cid']
		change_city(new_cid)

		x = coords[city]['lat_min']
		y = coords[city]['lon_min']
		lat_max = coords[city]['lat_max']
		lon_max = coords[city]['lon_max']
		lon_min = coords[city]['lon_min']

		url_params['cid'] = new_cid

		while x <= lat_max:

			while y <= lon_max:

				body['longitude'] = "%.2f" % y
				body['latitude'] = "%.2f" % x

				r = requests.post('http://indriver.ru/api/getfreedrivers', 
					data = body, 
					headers={'Content-Type':'application/x-www-form-urlencoded',
					'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:22.0) Gecko/20100101 Firefox/22.0'},
					params = url_params)

				try:
					data = r.json()
				except:
					continue

				item_count = len(data['response']['items'])
				print('| City:%s | items:%d' % (city, item_count))

				if item_count != 0:

					for i in range(item_count):

						query = ("SELECT COUNT(1) FROM Main WHERE firstname = %s and cargosnomer = %s and lastname = %s;")

						query_data = (data['response']['items'][i]['firstname'], 
							data['response']['items'][i]['cargosnomer'], 
							data['response']['items'][i]['lastname'])

						cursor.execute(query, query_data)

						if cursor.fetchone()[0] == 0:

							birthday = data['response']['items'][i]['birthday']
							birthday_mysql = None
							if birthday:
								birthday = birthday[:-6]
								birthday_format = '%a, %d %b %Y %H:%M:%S'

								birthday_mysql = datetime.strptime(birthday, birthday_format).date()

							insert_query = """INSERT INTO Main 
							(firstname,lastname, birthday,carname,carmodel,carcolor,cargosnomer,caryear,city) 
							 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

							insert_data = (
								data['response']['items'][i]['firstname'], 
								data['response']['items'][i]['lastname'],
								birthday_mysql,
								data['response']['items'][i]['carname'],
								data['response']['items'][i]['carmodel'],
								data['response']['items'][i]['carcolor'],
								data['response']['items'][i]['cargosnomer'],
								int(data['response']['items'][i]['caryear']),
								city
							)
							cursor.execute(insert_query, insert_data)
						else:
							continue
						
							

					conn.commit()
				y += step	

			x += step
			#print('SLeeping 5 seconds...')
			#time.sleep(5)
			y = lon_min

	print('Sleeping 60 minutes...')
	time.sleep(300)

cursor.close()
conn.close()

