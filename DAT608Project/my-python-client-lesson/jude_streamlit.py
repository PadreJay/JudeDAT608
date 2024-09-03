import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'jude_topic',  # Ensure this matches the topic used in your producer
    bootstrap_servers=['broker:39092'],  # Update with your Kafka server address
    auto_offset_reset='earliest', 
    enable_auto_commit=True  # Automatically commit offsets
)

def consume_kafka_data(limit=100):
    data_list = []
    for message in consumer:
        flattened_data = flatten_json(json.loads(message.value))
        data_list.append(flattened_data)
        if len(data_list) >= limit:  # Limit the number of messages processed
            break
    return data_list

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

def visualize_data(data):
    # Convert the data into a DataFrame
    df = pd.DataFrame(data)
    print(df)

    # Visualization 1: Orbital Parameters Scatter Plot
    st.subheader('Orbital Parameters Visualization')
    fig1 = px.scatter(df, x='spaceTrack_INCLINATION', y='spaceTrack_MEAN_MOTION', 
                      color='spaceTrack_OBJECT_NAME', 
                      title='Inclination vs Mean Motion',
                      labels={'spaceTrack_INCLINATION':'Inclination (degrees)', 
                              'spaceTrack_MEAN_MOTION':'Mean Motion (revs per day)'})
    st.plotly_chart(fig1)

    # Visualization 2: Ground Track Map
    st.subheader('Ground Track Visualization')
    fig2 = px.scatter_geo(df, lat='latitude', lon='longitude', 
                          color='spaceTrack_OBJECT_NAME',
                          title='Satellite Ground Track',
                          labels={'latitude':'Latitude', 'longitude':'Longitude'})
    st.plotly_chart(fig2)

    # Visualization 3: Velocity Distribution
    st.subheader('Velocity Distribution')
    fig3 = px.histogram(df, x='velocity_kms', 
                        title='Velocity Distribution of Satellites (km/s)',
                        labels={'velocity_kms':'Velocity (km/s)'})
    st.plotly_chart(fig3)

    # Visualization 4: Periapsis and Apoapsis
    st.subheader('Apoapsis and Periapsis Comparison')
    fig4 = px.bar(df, x='spaceTrack_OBJECT_NAME', y=['spaceTrack_APOAPSIS', 'spaceTrack_PERIAPSIS'],
                  title='Apoapsis and Periapsis Heights (km)',
                  labels={'value':'Height (km)', 'spaceTrack_OBJECT_NAME':'Object Name'},
                  barmode='group')
    st.plotly_chart(fig4)

# Streamlit App
st.title('Space Object Data Visualization')

# Consume data from Kafka
data_list = []
for data in consume_kafka_data():
    data_list.append(data)
    if len(data_list) >= 300:  # Limit to the first 100 messages for visualization
        break

# Visualize the data
if data_list:
    visualize_data(data_list)
else:
    st.write("No data available to display.")
