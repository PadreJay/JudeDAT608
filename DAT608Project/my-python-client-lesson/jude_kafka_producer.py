import logging
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Function to fetch data from the API
def fetch_api_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()  # Return the JSON data
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching data from API: {}", e)
        return None

# Function to send data to Kafka
def send_to_kafka(data):
    try:
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['broker:39092'],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

        for datum in data:
            # Send the API data to the 's1' topic
            producer.send('jude_topic', datum)
            print(datum)

        # Flush and close the producer
        producer.flush()
        producer.close()

    except KafkaError as e:
        logging.error("Error sending to Kafka broker: {}", e)
        producer.close()
        exit(1)

# URL of the API endpoint
api_url = 'https://api.spacexdata.com/v4/starlink'  # Replace with your actual API URL

# Fetch the data from the API
api_data = fetch_api_data(api_url)

# If data was successfully fetched, send it to Kafka
if api_data:
    send_to_kafka(api_data)
else:
    logging.error("No data to send to Kafka")

