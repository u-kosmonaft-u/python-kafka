from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='192.168.10.50:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    producer.send('test', {'foo': 'bar'})

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
