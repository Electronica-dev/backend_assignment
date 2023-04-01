from fastapi import FastAPI
from dotenv import load_dotenv
from os import getenv
from typing import List
from dateutil import parser
from datetime import timedelta
import mysql.connector
from concurrent.futures import ProcessPoolExecutor

load_dotenv()
app = FastAPI()
executor = ProcessPoolExecutor()

PASSWORD = getenv('PASSWORD')

connection = mysql.connector.connect(host='127.0.0.1', port=3306,
																		user='root', password=PASSWORD,
																		db='backend_app')

QUERY_STORE_HOURS = ("SELECT day, start_time_local, end_time_local FROM menu_hours WHERE store_id=%s ORDER BY day")
#Currently the date is set to 26th of January (UTC). It can be changed to the current date and time using 'NOW()'
QUERY_STORE_STATUS_WEEK = '''
														SELECT status, timestamp_utc,
														IF (@converted := CONVERT_TZ(timestamp_utc, 'UTC', %s), @converted, NULL) AS timestamp_local,
														WEEKDAY(@converted)
														FROM store_status
														WHERE store_id=%s
														HAVING timestamp_local >= DATE_SUB('2023-01-25 13:00:00', INTERVAL 1 WEEK)
														ORDER BY timestamp_local
													'''
QUERY_STORE_STATUS_DAY =  '''
														SELECT status, timestamp_utc,
														IF (@converted := CONVERT_TZ(timestamp_utc, 'UTC', %s), @converted, NULL) AS timestamp_local,
														WEEKDAY(@converted)
														FROM store_status
														WHERE store_id=%s
														HAVING timestamp_local >= DATE_SUB('2023-01-25 13:00:00', INTERVAL 1 DAY)
														ORDER BY timestamp_local
													'''
QUERY_STORE_STATUS_DAY_24_7 = '''
																SELECT COUNT(*)
																FROM store_status
																WHERE store_id=%s
																AND CONVERT_TZ(timestamp_utc, 'UTC', %s) >= DATE_SUB('2023-01-25 13:00:00', INTERVAL 1 DAY);
															'''
QUERY_STORE_STATUS_HOUR = '''
														SELECT status, timestamp_utc,
														IF (@converted := CONVERT_TZ(timestamp_utc, 'UTC', %s), @converted, NULL) AS timestamp_local,
														WEEKDAY(@converted)
														FROM store_status
														WHERE store_id=%s
														HAVING timestamp_local >= DATE_SUB('2023-01-25 13:00:00', INTERVAL 1 HOUR)
														ORDER BY timestamp_local;
													'''
QUERY_STORE_TIMEZONE = ("SELECT timezone_str FROM bq_results WHERE store_id=%s")

def find_tz(store_id: int) -> str:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_TIMEZONE, (store_id, ))
	time_zone = cursor.fetchone()
	return str.strip(time_zone[0]) if time_zone else 'America/Chicago'

def find_week_status(time_zone: str, store_id: int) -> List:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_STATUS_WEEK, (time_zone, store_id))
	return cursor.fetchall()

def find_day_status(time_zone: str, store_id: int) -> List:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_STATUS_DAY, (time_zone, store_id))
	return cursor.fetchall()

def find_day_24_7_status(store_id, time_zone) -> int:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_STATUS_DAY_24_7, (store_id, time_zone))
	return cursor.fetchone()

def find_hour_status(time_zone: str, store_id: int) -> List:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_STATUS_HOUR, (time_zone, store_id))
	return cursor.fetchone()

#Returns a list of dictionaries with 0th index being 'from' timings and 1st index being 'to' timings.
def find_store_hours(store_id: int) -> List:
	cursor = connection.cursor()
	cursor.execute(QUERY_STORE_HOURS, (store_id, ))
	res = cursor.fetchall()
	return res

def convert_datetime_to_timedelta(dt):
	td = timedelta(
		hours=dt.hour,
		minutes=dt.minute,
		seconds=dt.second
	)
	return td

def count_day_or_week_hours(week_store_status, day, store_open_time, store_close_time, active, inactive):
	for status, utc_timestamp, local_timestamp, weekday in week_store_status:
		if day == weekday:

			store_open_timedelta = convert_datetime_to_timedelta(store_open_time)
			store_close_timedelta = convert_datetime_to_timedelta(store_close_time)

			local_timestamp = parser.parse(local_timestamp)
			local_timestamp_timedelta = convert_datetime_to_timedelta(local_timestamp)
			if (
					local_timestamp_timedelta >= store_open_timedelta and
					local_timestamp_timedelta <= store_close_timedelta
				):
					active += 1

	return [active, inactive]