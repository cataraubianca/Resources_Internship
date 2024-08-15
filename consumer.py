from kafka import KafkaConsumer, TopicPartition
import json
import pandas as pd
import matplotlib.pyplot as plt
import time
from datetime import datetime


messages = []

consumer = KafkaConsumer(
    'training1600000processednoemoticon',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000000
)

partitions = consumer.partitions_for_topic('training1600000processednoemoticon')
if partitions:
    for partition in partitions:
        tp = TopicPartition('training1600000processednoemoticon', partition)
        consumer.seek_to_end(tp)

def plot_arrival_times_unix(arrival_times_unix):
    plt.figure(figsize=(10, 6))

    counts = arrival_times_unix.value_counts().sort_index()

    counts.plot(kind='bar', color='skyblue', edgecolor='black')

    plt.title('Counts of Message Arrival Times')
    plt.xlabel('Time')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.grid(True)

    step = max(1, len(counts) // 10)
    plt.xticks(ticks=range(0, len(counts), step),
               labels=[datetime.fromtimestamp(counts.index[i]).strftime('%Y-%m-%d %H:%M:%S')
                       for i in range(0, len(counts), step)])

    plt.tight_layout()
    plt.show()

try:
    print("Starting to consume messages...")
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))  
        print(data)

        if 'end_of_transmission' in data:
            print("End of transmission signal received.")
            break

        current_time = datetime.now()
        current_time_unix = int(current_time.timestamp())
        messages.append({'arrival_time_unix': current_time_unix, **data})
finally:
    consumer.close()

if messages:
    df = pd.DataFrame(messages)
    plot_arrival_times_unix(df['arrival_time_unix'])
else:
    print("No messages received.")
