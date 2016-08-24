from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('iot', 
	group_id='thucdx-python-group2',
	bootstrap_servers=['slave7', 'slave8', 'slave9'])

i = 0
for msg in consumer:
  # print msg.value
  try:
  	  json_data = json.loads(msg.value)

	  if "device_type" in json_data and json_data["device_type"] == 'facial_sensing':
	    print "Age: {0}, Gender: {1}, Expression: {2}".format(json_data["age"], json_data["gender"], json_data["face_expression"])
	  i += 1
  finally:
  	pass

print 'total ', i

