from kafka import KafkaConsumer

consumer = KafkaConsumer(group_id='pythonGroup',
                         bootstrap_servers=['192.168.10.60:9092', '192.168.10.61:9092', '192.168.10.62:9092'])
consumer.subscribe(['pythonTopic'])
for msg in consumer:
    print(msg)
