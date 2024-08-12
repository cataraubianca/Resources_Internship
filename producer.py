import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from pymongo import MongoClient
import sys
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from kafka import KafkaProducer
import json
import time
import subprocess
import codecs
import math

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

DATETIME_FORMATS = [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '[%d/%b/%Y:%H:%M:%S',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d',
    '%d-%m-%Y %H:%M:%S',
    '%d-%m-%Y %H:%M',
    '%d-%m-%Y',
    '%m/%d/%Y %H:%M:%S',
    '%m/%d/%Y %H:%M',
    '%m/%d/%Y',
    '%d/%m/%Y %H:%M:%S',
    '%d/%m/%Y %H:%M',
    '%d/%m/%Y',
    '%Y/%m/%d %H:%M:%S',
    '%Y/%m/%d %H:%M',
    '%Y/%m/%d',
    '%H:%M:%S',
    '%H:%M',
    '%a %b %d %H:%M:%S %Z %Y',
    '%Y-%m-%d %H:%M:%S%z',
    '%a %b %d %H:%M:%S %Z %Y',
    '%a %b %d %H:%M:%S PDT %Y',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%dT%H:%M:%S%z',
]

def to_unix_timestamp_rounded(timestamp):
    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(timestamp, fmt)
            dt = dt.replace(second=0, microsecond=0)
            return int(dt.timestamp()) * 1000
        except ValueError:
            continue
    return None

def start_zookeeper_and_kafka():
    try:
        zookeeper_process = subprocess.Popen(['cmd', '/c', 'zookeeper.bat'], shell=True)
        print("Starting Zookeeper...")
        time.sleep(60)  
        
        kafka_process = subprocess.Popen(['cmd', '/c', 'kafka.bat'], shell=True)
        print("Starting Kafka...")
        time.sleep(60) 
        
        return kafka_process
    except Exception as e:
        print(f"Error starting Zookeeper/Kafka: {e}")
        sys.exit(1)

def cleanup(producer, kafka_process):
    if producer is not None:
        producer.flush()

    if kafka_process is not None:
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(kafka_process.pid)])

def wait_for_kafka(bootstrap_servers, retries=5, delay=10):
    from kafka import KafkaAdminClient
    for i in range(retries):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin_client.list_topics()
            print("Kafka broker is available.")
            return True
        except Exception as e:
            print(f"Kafka broker not available yet. Retrying in {delay} seconds... ({i+1}/{retries})")
            time.sleep(delay)
    print("Failed to connect to Kafka broker after several attempts.")
    return False

def main():

    kafka_process = start_zookeeper_and_kafka()
    if not wait_for_kafka('localhost:9092'):
        print("Exiting due to Kafka broker availability issues.")
        sys.exit(1)
    time.sleep(30)

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=serializer
    )
    divide = input("Please enter the parameter for time division: ")

    amplifier = int(input("Please enter the amplifier value: "))

    csv_file_path = 'training1600000processednoemoticon.csv'
    
    encodings = ['utf-8', 'latin-1', 'ISO-8859-1']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_file_path, encoding=encoding)
            print(f"Successfully loaded CSV file using encoding: {encoding}")
            break  
        except UnicodeDecodeError as e:
            print(f"Error decoding with encoding {encoding}: {str(e)}")
            continue  
    
    if df is None:
        print("Unable to load CSV file with any encoding. Exiting.")
        return

    df.columns = [col.lower() for col in df.columns]
    print("Columns:", df.columns)
    
    num_columns = len(df.columns)
    print("Number of columns:", num_columns)

    timestamp_col = find_timestamp_column(df)
    if timestamp_col is None: 
        print("No suitable timestamp column found in the CSV file.")
        return

    print("Timestamp column:", timestamp_col)

    df_filtered = df[df.apply(lambda row: is_valid_timestamp(row[timestamp_col]), axis=1)]
    
    df_filtered.sort_values(by=timestamp_col, inplace=True)

    df_filtered['modified_timestamp'] = df_filtered[timestamp_col].apply(to_unix_timestamp_rounded)
    
    df_filtered.drop_duplicates(inplace=True)
    print("Number of rows after dropping duplicates:", len(df_filtered))
    
    
    output_file_path = 'modified_data_training1600000processednoemoticon.csv'
    df_filtered.to_csv(output_file_path, index=False, encoding='utf-8')  
    print(f"Modified CSV file has been saved as {output_file_path}")

    add_to_mongodb(df_filtered, csv_file_path)
    plot_modified_timestamps(df_filtered['modified_timestamp'])

    start_timestamp = input("Please enter the start timestamp: ")
    end_timestamp = input("Please enter the end timestamp: ")
    
    if start_timestamp is None or end_timestamp is None:
        print("Invalid start or end timestamp format.")
        return

    print(f"Filtering data between Unix timestamps {start_timestamp} and {end_timestamp}.")
    
    df_filtered = df_filtered[
        (df_filtered['modified_timestamp'] >= int(start_timestamp)) &
        (df_filtered['modified_timestamp'] <= int(end_timestamp))
    ]
    
    topic_name = csv_file_path.split('.')[0] 
    create_kafka_topic('localhost:9092', topic_name, 1) 
    p = inject_to_kafka(producer, df_filtered, topic_name, divide, amplifier)

    cleanup(p, kafka_process)


def find_timestamp_column(df):
    for col in df.columns:
        try:
            sample_values = df[col].dropna().head().astype(str).tolist()
            print(f"Checking column '{col}' for standard timestamps.")
            if all(is_valid_timestamp(value) for value in sample_values):
                return col
        except IndexError:
            continue
    
    return None

def is_valid_timestamp(value):
    value_str = str(value)
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(value_str, fmt)
            return True
        except ValueError:
            continue
    return False

def plot_modified_timestamps(timestamps):
    plt.figure(figsize=(12, 6))

    counts = timestamps.value_counts().sort_index()

    first_timestamp = counts.index[0]

    plt.bar(counts.index, counts.values, color='skyblue', edgecolor='black')

    plt.title('Counts of Modified Unix Timestamps')
    plt.xlabel('Unix Timestamp')
    plt.ylabel('Count')

    step = max(1, len(counts) // 10)
    labels = [f"{int(ts)}\n+{int(ts - first_timestamp)}" for ts in counts.index[::step]]
    plt.xticks(
        ticks=counts.index[::step], 
        labels=labels, 
        rotation=45
    )

    plt.grid(True)
    plt.tight_layout()
    plt.show()




def add_to_mongodb(df, csv_file_path):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['my_database']
    collection_name = csv_file_path.split('.')[0] 
    collection = db[collection_name]
    
    if collection_name in db.list_collection_names():
        print(f"Dropping existing collection '{collection_name}'...")
        db.drop_collection(collection_name)
    
    records = df.to_dict('records')
    collection.insert_many(records)
    print(f"Data has been added to MongoDB collection '{collection_name}'")

def create_kafka_topic(bootstrap_servers, topic_name, num_partitions):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=1)
    
    fs = admin_client.create_topics([new_topic])
    
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except KafkaException as e:
            print(f"Failed to create topic '{topic}': {e}")
            if e.args[0].code() == 36:
                print(f"Topic '{topic}' already exists.")

def serializer(message):
    return json.dumps(message).encode('utf-8')

def inject_to_kafka(producer, df, topic, divide, amplifier):
    sorted_df = df.sort_values(by='modified_timestamp')
    i = 0
    
    for index, row in sorted_df.iterrows():
        current_timestamp = row['modified_timestamp']
        if i == 0:
            previous_timestamp = row['modified_timestamp']
        i += 1
        if previous_timestamp is not None:
            time_difference = (current_timestamp - previous_timestamp)/1000
            if time_difference > 0:
                print(f"Waiting for {time_difference} seconds before sending the next message...")
                time.sleep(time_difference / int(divide))
        
        message = {
            'timestamp': current_timestamp, 
            'data': row.to_dict()  
        }

        print(json.dumps(message, ensure_ascii=False))
        
        for _ in range(amplifier):
            producer.send(topic, value=message)
        
        previous_timestamp = current_timestamp

    end_of_transmission_message = {'end_of_transmission': True}
    producer.send(topic, value=end_of_transmission_message)
    print(json.dumps(end_of_transmission_message, ensure_ascii=False))

    producer.flush()
    return producer

if __name__ == "__main__":
    main()
