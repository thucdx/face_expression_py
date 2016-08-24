from flask import Flask
from kafka import KafkaConsumer
import threading
import json
import atexit

age = 'no data'
gender = 'no data'
expression = 'no data'

consumer = KafkaConsumer('iot', 
		group_id='python-demo',
		bootstrap_servers=['slave7', 'slave8', 'slave9'])	

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/data")
def get_date():
	global age, gender, expression
	print age, gender, expression
	return '{\"age\": \"%s\", \"gender\": \"%s\", \"expression\":  \"%s\" }' % (age, gender, expression)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def fetch_message():
	print 'Fetching '
	# Message 
	for msg in consumer:
		try :
			json_data = json.loads(msg.value)
			if "device_type" in json_data and json_data["device_type"] == "facial_sensing":
				global age, gender, expression
				age = json_data["age"]
				gender = json_data["gender"]
				expression = json_data["face_expression"]

				print "GOT: ", age, gender, expression
				print "Age: {0}, Gender: {1}, Expression: {2}".format(json_data["age"], json_data["gender"], json_data["face_expression"])
		except:
			print 'Life goes on!'

t = threading.Thread(target=fetch_message)
def interrupt():
	global t
	t.cancel()

t.start()

if __name__ == "__main__":
    app.run()
    atexit.register(interrupt)
