from kafka import KafkaProducer
import json
import time
from numpy.random import choice, randint


producer = KafkaProducer(bootstrap_servers=['192.168.10.60:9092', '192.168.10.61:9092', '192.168.10.62:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
def get_random_value():
    new_dict = {}

    city_list = ['New York', 'Los Angeles', 'Chicago',
                 'Houston', 'Philadelphia', 'Moscow',
                 'London', 'Santiago', 'Paris']

    currency_list = ['RUB', 'USD', 'EUR', 'GBP']

    new_dict['branch'] = choice(city_list)
    new_dict['currency'] = choice(currency_list)
    new_dict['amount'] = randint(-100, 100)

    return new_dict

while True:
    try:
        data = get_random_value()
        json_data = json.dumps( data )
        future = producer.send( 'pythonTopic', json_data )
        record_metadata = future.get( timeout=10 )

        print( '--> The message has been sent to a topic: \
                {}, partition: {}, offset: {}' \
               .format( record_metadata.topic,
                        record_metadata.partition,
                        record_metadata.offset ) )

    except Exception as e:
        print( '--> It seems an Error occurred: {}'.format( e ) )

    finally:
        producer.flush()
        time.sleep( 1.5 )
