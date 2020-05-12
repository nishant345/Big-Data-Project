from kafka import KafkaProducer
import requests
from configparser import ConfigParser
import json
import logging as log
import time
import datetime

from application.util import string_to_list

# config parser
parser = ConfigParser()
parser.read('config.ini')

# configuring log
logging_app = parser.get('log', 'logging_app')
if logging_app == '1':
    log.basicConfig(
        filename=parser.get('log', 'filename'),
        level=log.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

producer = None
json_msg = None
app_id = None
city = None
now_time = datetime.datetime.now()
future_time = now_time + datetime.timedelta(seconds=10)

# Kafka Producer settings
log.info("********------------------------KAFKA SETTINGS----------------------------------------------********")
kafka_broker = parser.get('kafka_producer', 'kafka_broker')
kafka_topic = parser.get('kafka_producer', 'kafka_topic')
log.info(" Kafka Broker connected to: %s with topic: %s" % (kafka_broker, kafka_topic))
log.info("********-------------------------------------------------------------------------------------********")

# Data Source Settings
log.info("********------------------------DATA SOURCE SETTINGS-----------------------------------------********")
api_url = parser.get('data_source', 'api_url')
api_id = parser.get('data_source', 'api_id')
cities = parser.get('data_source', 'query_city')
cities = string_to_list(cities)
log.info(" Fetching data from source: %s for city: %s" % (api_url, cities))
log.info("********-------------------------------------------------------------------------------------********")

# initializing Kafka Producer
log.info("********------------------------INITIALIZING KAFKA--------------------------------------------********")

try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        api_version=(0, 10),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Connection to Kafka  Broker succeeded")
    log.info("Connection to Kafka Broker succeeded with: %s with topic: %s" % (kafka_broker, kafka_topic))
except Exception as ex:
    print('Exception while connecting Kafka')
    print(str(ex))


def getWeatherDetails(city, app_id):
    api_response = requests.get(api_url+'?q=%s&appid=%s' % (city, api_id))
    json_data = api_response.json()
    city_name = json_data['name']
    humidity = json_data['main']['humidity']
    temperature = json_data['main']['temp']
    json_value = {
        'city_name': city_name,
        'temperature': temperature,
        'humidity': humidity,
        'creation_time': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    return json_value


log.info("********------------------------SENDING DATA TO KAFKA BROKER---------------------------------------********")
while now_time != future_time:
    for city in cities:
        json_msg = getWeatherDetails(city, app_id)
        # producer.send(kafka_topic, json_msg)
        # producer.flush()
    now_time = now_time + datetime.timedelta(seconds=2)

log.info("Weather details saved to Kafka topic {} for cities: {}".format(kafka_topic, cities))
print("Weather details saved to Kafka topic {} for cities: {}".format(kafka_topic, cities))



