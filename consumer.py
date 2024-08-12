from kafka import KafkaConsumer, TopicPartition
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

messages = []

consumer = KafkaConsumer(
    'training1600000processednoemoticon',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

partitions = consumer.partitions_for_topic('training1600000processednoemoticon')
if partitions:
    for partition in partitions:
        tp = TopicPartition('training1600000processednoemoticon', partition)
        consumer.seek_to_end(tp)

def plot_arrival_times_unix(arrival_times_unix):
    plt.figure(figsize=(12, 6))

    counts = arrival_times_unix.value_counts().sort_index()

    x_values = range(len(counts))

    plt.bar(x_values, counts.values, color='lightcoral', edgecolor='black', width=0.8)

    plt.title('Counts of Message Arrival Times')
    plt.xlabel('Messages')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.grid(True)

    first_timestamp = counts.index[0]
    normalized_timestamps = counts.index - first_timestamp
    plt.xticks(ticks=x_values[::max(1, len(x_values) // 10)], 
               labels=[f"+{ts} ms" for ts in normalized_timestamps[::max(1, len(x_values) // 10)]])

    plt.tight_layout()
    plt.show()



try:
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))

        if 'end_of_transmission' in data:
            print("End of transmission signal received.")
            break

        current_time = datetime.now()
        current_time_milliseconds = int(current_time.timestamp() * 1000)
        messages.append({'arrival_time_unix': current_time_milliseconds, **data})
finally:
    consumer.close()

if messages:
    df = pd.DataFrame(messages)
    plot_arrival_times_unix(df['arrival_time_unix'])
else:
    print("No messages received.")
