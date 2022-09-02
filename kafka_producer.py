from time import sleep
from json import dumps
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for n in range(10):
    data = {'number' : n}
    producer.send('my_kafka_test', value=data)
    print(n)
    sleep(5)
