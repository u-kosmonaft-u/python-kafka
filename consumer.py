from kafka import KafkaConsumer

consumer = KafkaConsumer('test', bootstrap_servers='192.168.10.50:9092')

for msg in consumer:
    print(msg)
