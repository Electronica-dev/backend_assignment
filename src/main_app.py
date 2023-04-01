import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from tasks import enqueue_store_status_calculations
from dotenv import load_dotenv
from os import getenv
import mysql.connector

load_dotenv()
app = FastAPI()

QUERY_TASK_STATUS = "SELECT status FROM celery_taskmeta WHERE task_id=%s"

PASSWORD = getenv('PASSWORD')

csv_file_path = 'C:\\Users\\Sammy\\Desktop\\backend_assignment\\db\\'

connection = mysql.connector.connect(host='127.0.0.1', port=3306,
																		user='root', password=PASSWORD,
																		db='backend_app')

@app.get('/trigger_report')
def get_store_status():
	celery_task = enqueue_store_status_calculations.apply_async()
	return celery_task.id

@app.get('/get_report')
def return_file_if_exists(report_id):
	cursor = connection.cursor()
	cursor.execute(QUERY_TASK_STATUS, (report_id, ))
	is_running = cursor.fetchone()
	if is_running[0] == "SUCCESS":
		connection.commit()
		return FileResponse(csv_file_path + report_id + '.csv', filename=report_id + '.csv', media_type="text/csv")
	else:
		connection.commit()
		return 'Running'

if __name__ == '__main__':
	uvicorn.run("main_app:app", port = 8000)