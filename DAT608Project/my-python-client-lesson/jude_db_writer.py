from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import re
import json

# Setup logging
logging.basicConfig(level=logging.INFO)

# Get mandatory connection
conn = getConnection(True)

def sanitize_text(text):
    if text is None:
        return None
    # Remove non-printable characters except for standard whitespace
    return re.sub(r'[^\x20-\x7E]', '', text)

def flatten_json(nested_json):
    """Flattens a nested JSON object"""
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def setup_database():
    """Create the table if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS starlink_data (
                    id SERIAL PRIMARY KEY,
                    object_name TEXT,
                    object_id TEXT,
                    mean_motion FLOAT,
                    eccentricity FLOAT,
                    inclination FLOAT,
                    apoapsis FLOAT,
                    periapsis FLOAT,
                    height_km FLOAT,
                    latitude FLOAT,
                    longitude FLOAT,
                    velocity_kms FLOAT,
                    creation_date TIMESTAMP
                )
            """)
            conn.commit()
            logging.info("Table created or already exists.")
    except Exception as e:
        logging.error(f"Problem setting up the database: {e}")

def cockroachWrite(event):
    """Write Kafka event to the database."""
    try:
        eventValue = json.loads(event.value)
        
        # Flatten the JSON
        flat_event = flatten_json(eventValue)

        # Sanitize the data
        flat_event = {key: sanitize_text(str(value)) if isinstance(value, str) else value for key, value in flat_event.items()}

        # Extract necessary fields
        object_name = flat_event.get('spaceTrack_OBJECT_NAME')
        object_id = flat_event.get('spaceTrack_OBJECT_ID')
        mean_motion = flat_event.get('spaceTrack_MEAN_MOTION')
        eccentricity = flat_event.get('spaceTrack_ECCENTRICITY')
        inclination = flat_event.get('spaceTrack_INCLINATION')
        apoapsis = flat_event.get('spaceTrack_APOAPSIS')
        periapsis = flat_event.get('spaceTrack_PERIAPSIS')
        height_km = flat_event.get('height_km')
        latitude = flat_event.get('latitude')
        longitude = flat_event.get('longitude')
        velocity_kms = flat_event.get('velocity_kms')
        creation_date = flat_event.get('spaceTrack_CREATION_DATE')

        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO starlink_data (object_name, object_id, mean_motion, eccentricity, inclination, 
                                           apoapsis, periapsis, height_km, latitude, longitude, velocity_kms, creation_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (object_name, object_id, mean_motion, eccentricity, inclination, apoapsis, 
                  periapsis, height_km, latitude, longitude, velocity_kms, creation_date))
            conn.commit()
            logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Problem writing to database: {e}")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'jude_topic',  # Ensure this matches the topic used in your producer
    bootstrap_servers=['broker:39092'],  # Update with your Kafka server address
    auto_offset_reset='earliest'
)

# Set up the database table
setup_database()

# Consume Kafka messages and write to the database
for msg in consumer:
    cockroachWrite(msg)
