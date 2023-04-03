import datetime
import csv
import pytz

from celery import Celery
from celery import group
from dotenv import load_dotenv
from os import getenv
from helper import convert_datetime_to_timedelta
from helper import count_day_or_week_hours
from helper import find_day_status
from helper import find_week_status
from helper import find_store_hours
from helper import find_tz
from helper import find_day_24_7_status
from helper import find_hour_status
from helper import find_store_ids
from datetime import timedelta, timezone
from dateutil import parser
from typing import List
from typing import Union
from typing import Tuple

load_dotenv()
PASSWORD=getenv('PASSWORD')

CELERY_BROKER_URL = 'amqp://guest:guest@localhost:5672//'
RESULT_BACKEND = f'db+mysql+pymysql://root:{PASSWORD}@127.0.0.1:3306/backend_app'

app = Celery('tasks', broker=CELERY_BROKER_URL, backend=RESULT_BACKEND)

#Ensures that the task state can be retrieved from the database even if it has not yet finished running
app.conf.update(
	task_track_started=True
)

# INTERPOLATION LOGIC
# The main interpolation logic is simple:
# 1. Check the period for which the store is supposed to be active and store it in 'store_hours'.
# 2. Query the database and get the hourly timestamps and the status.
# 3. Convert the timezone to the local timezone as the timestamps are in UTC.
# 4. If the timestamp falls under the period for which the store was meant to be open,
#		 add it to a variable which keeps track of active hours.
# 5. Now, if the number of timestamps are equal to the number of hours the store is meant to be open, well and good.
# 6. Else, get the inactive hours by subtracting the total amount the store was meant to be active.
#
# This changes a little bit when calculating the number of minutes a store was active for:
# In this case, we simply run a single query to check if the store was active and then extract the active and inactive
# minutes from it.

@app.task
def find_week_hours(store_id: int, store_hours: Tuple[Tuple[int, str, str]]) -> Union[List[Union[int, int]], None]:
	time_zone = find_tz(store_id)
	week_store_status = find_week_status(time_zone, store_id)

	if not week_store_status:
		return None

	store_open_24_7 = False
	if len(store_hours) == 0:
		store_open_24_7 = True

	active_week_hours = 0
	inactive_week_hours = 0

	if store_open_24_7:

		store_open_time = datetime.datetime.strptime('00:00:00', "%H:%M:%S")
		store_close_time = datetime.datetime.strptime('23:59:59', "%H:%M:%S")

		for day in range(7):

			hours_open_int = 24

			active_day_hours, inactive_day_hours = count_day_or_week_hours(week_store_status, day, store_open_time, store_close_time, -1, -1)

			if active_day_hours + inactive_day_hours <= hours_open_int:
				inactive_day_hours = hours_open_int - active_day_hours

			active_week_hours += active_day_hours
			inactive_week_hours += inactive_day_hours
	else:
		for day, store_open_time, store_close_time in store_hours:

			store_open_time = datetime.datetime.strptime(store_open_time, "%H:%M:%S")
			store_close_time = datetime.datetime.strptime(store_close_time, "%H:%M:%S")

			hours_open_timedelta = store_close_time - store_open_time
			hours_open_int = hours_open_timedelta.seconds // 3600

			active_day_hours, inactive_day_hours = count_day_or_week_hours(week_store_status, day, store_open_time, store_close_time, -1, -1)

			if active_day_hours + inactive_day_hours <= hours_open_int:
				inactive_day_hours = hours_open_int - active_day_hours

			active_week_hours += active_day_hours
			inactive_week_hours += inactive_day_hours
	return [active_week_hours, inactive_week_hours]

@app.task
def find_day_hours(store_id: int, store_hours: Tuple[Tuple[int, str, str]]) -> Union[List[Union[int, int]], None]:
	time_zone = find_tz(store_id)

	day_store_status = find_day_status(time_zone, store_id)

	if not day_store_status:
		return None

	store_open_24_7 = False
	if len(store_hours) == 0:
		store_open_24_7 = True

	active_day_hours = 0
	inactive_day_hours = 0

	if store_open_24_7:
		active_day_hours = find_day_24_7_status(store_id, time_zone)
		inactive_day_hours = 24 - active_day_hours
	else:
		for day, store_open_time, store_close_time in store_hours:

			store_open_time = datetime.datetime.strptime(store_open_time, "%H:%M:%S")
			store_close_time = datetime.datetime.strptime(store_close_time, "%H:%M:%S")

			hours_open_timedelta = store_close_time - store_open_time

			hours_open_int = hours_open_timedelta.seconds // 3600

			active_hours, inactive_hours = count_day_or_week_hours(day_store_status, day, store_open_time, store_close_time, -1, -1)

			if active_hours == -1 and inactive_hours == -1:
				continue

			if active_hours + inactive_hours <= hours_open_int:
				inactive_hours = hours_open_int - active_hours

			active_day_hours += active_hours
			inactive_day_hours += inactive_hours

	return [active_day_hours, inactive_day_hours]

@app.task
def find_hours(store_id: int, store_hours: Tuple[Tuple[int, str, str]]) -> Union[List[Union[int, int]], None]:
	time_zone = find_tz(store_id)
	query_res = find_hour_status(time_zone, store_id)

	if not query_res:
		return None

	status = query_res[0]
	timestamp_local = query_res[2]
	weekday = query_res[3]

	for day, store_open_time, store_close_time in store_hours:
		if day == weekday:

			store_open_time = datetime.datetime.strptime(store_open_time, "%H:%M:%S")
			store_close_time = datetime.datetime.strptime(store_close_time, "%H:%M:%S")

			store_open_timedelta = convert_datetime_to_timedelta(store_open_time)
			store_close_timedelta = convert_datetime_to_timedelta(store_close_time)

			if isinstance(timestamp_local, str):
				timestamp_local = parser.parse(timestamp_local)

			local_timestamp_timedelta = convert_datetime_to_timedelta(timestamp_local)

			if (
					local_timestamp_timedelta >= store_open_timedelta and
					local_timestamp_timedelta <= store_close_timedelta
				):
				#Current time is set to the maximum time + 1 hour in the utc time_zone
				utc_datetime = parser.parse('2023-01-25 19:00:00')

				local_timezone = pytz.timezone(time_zone)

				current_time_aware = utc_datetime.replace(tzinfo=timezone.utc).astimezone(tz=local_timezone)
				current_time_naive = current_time_aware.replace(tzinfo=None)

				if status == 'active':
					active_minutes_timedelta = current_time_naive - timestamp_local
					active_minutes = active_minutes_timedelta.seconds // 60
					return [active_minutes, 60 - active_minutes]

				elif status == 'inactive':
					inactive_minutes_timedelta = current_time_naive - timestamp_local
					inactive_minutes = inactive_minutes_timedelta.seconds // 60
					return [60 - inactive_minutes, inactive_minutes]

def fromat_timedelta_to_hhmmss(td: timedelta) -> str:
	td_in_seconds = td.total_seconds()
	hours, remainder = divmod(td_in_seconds, 3600)
	minutes, seconds = divmod(remainder, 60)
	hours = int(hours)
	minutes = int(minutes)
	seconds = int(seconds)
	if minutes < 10:
			minutes = "0{}".format(minutes)
	if seconds < 10:
			seconds = "0{}".format(seconds)
	return "{}:{}:{}".format(hours, minutes,seconds)

@app.task(bind=True)
def enqueue_store_status_calculations(self) -> str:
	store_id_list = find_store_ids()

	csv_file = open(f'C:\\Users\\Sammy\\Desktop\\backend_assignment\\db\\{self.request.id}.csv', 'w', newline='')

	header = ['store_id',
						'uptime_last_hour',
						'uptime_last_day',
						'uptime_last_week',
						'downtime_last_hour',
						'downtime_last_day',
						'downtime_last_week'
						]
	writer = csv.writer(csv_file)
	writer.writerow(header)

	active_week_hours = active_day_hours = active_hours = 0
	inactive_week_hours = inactive_day_hours = inactive_hours = 0

	for i in range(len(store_id_list)):

		store_id = store_id_list[i][0]

		store_hours = find_store_hours(store_id)
		store_hours = [
										(weekday, fromat_timedelta_to_hhmmss(open_from), fromat_timedelta_to_hhmmss(open_till))
										for weekday, open_from, open_till in store_hours
									]

		find_hours_result = group([
			find_week_hours.s(store_id, store_hours),
			find_day_hours.s(store_id, store_hours),
			find_hours.s(store_id, store_hours)
		]).apply_async()

		result = find_hours_result.get()

		if result[0]:
			active_week_hours, inactive_week_hours = result[0]
			active_day_hours, inactive_day_hours = result[1]
			active_hours, inactive_hours = result[2]
		else:
			continue

		row = [store_id, active_hours, active_day_hours, active_week_hours, inactive_hours, inactive_day_hours, inactive_week_hours]
		writer.writerow(row)

	csv_file.close()
	return 'completed'