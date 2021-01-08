import json
import redis
from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'realtime',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    count = 1
    client = redis.Redis('localhost', 6379)
    for msg in consumer:
        print(f"fetch message {count}")
        value = {
            "lat": msg.value["Position.lat"],
            "lon": msg.value["Position.lon"],
            "altitude": msg.value["Position.altitude"],
            "heading": msg.value["Position.heading"],
            "speed": msg.value["Position.speed"]
        }
        client.rpush(msg.value["VinVehicle"], json.dumps(value).encode('utf-8'))
        count = count + 1

