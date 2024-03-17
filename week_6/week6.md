
# Stream-Processing with Python

In this document, you will be finding information about stream processing 
using different Python libraries (`kafka-python`,`confluent-kafka`,`pyspark`, `faust`).

This Python module can be separated in following modules.

## 1. Docker
Docker module includes, Dockerfiles and docker-compose definitions 
to run Kafka and Spark in a docker container. Setting up required services is
the prerequsite step for running following modules



### Running Spark and Kafka Clusters on Docker

#### 1.1. Build Required Images for running Spark

The details of how to spark-images are build in different layers can be created can be read through 
the blog post written by André Perez on [Medium blog -Towards Data Science](https://towardsdatascience.com/apache-spark-cluster-on-docker-ft-a-juyterlab-interface-418383c95445)

The bash file and all connected docker image files are here - ```week_6_streaming_processing\docker\spark\build.sh```


```bash
# Build Spark Images
./build.sh 
```

#### 1.2. Create Docker Network & Volume

```bash
# Create Network
docker network  create kafka-spark-network

# Create Volume
docker volume create --name=hadoop-distributed-file-system
```

#### 1.3. Run Services on Docker
```bash
# Start Docker-Compose (first - within kafka folder and then - in spark folder)
docker compose up -d
```
We should have now:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/kafka_py5.png)
- Confluent Control center (UI) will be in localhost:9021 (kafka)

In depth explanation of [Kafka Listeners](https://www.confluent.io/blog/kafka-listeners-explained/)

Explanation of [Kafka Listeners](https://www.confluent.io/blog/kafka-listeners-explained/)

#### 1.4. Stop Services on Docker
```bash
# Stop Docker-Compose (within for kafka and spark folders)
docker compose down
```

#### 1.5. Helpful Comands
```bash
# Delete all Containers
docker rm -f $(docker ps -a -q)

# Delete all volumes
docker volume rm $(docker volume ls -q)
```

## 2. Kafka Producer - Consumer Examples
### [Json Producer-Consumer Example](json_example) using `kafka-python` library

#### 2.1. Install libraries
```
pip install -r requirements.txt
```
The ```requirements.txt``` file consists:

```
kafka-python==1.4.6
confluent_kafka
requests
avro
faust
fastavro
```

#### 2.2. Run producer

```
python producer.py
```
The ```producer.py``` file is:
```
import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header row
            for row in reader:
                records.append(Ride(arr=row))
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        for ride in messages:
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)

```
We should get this:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/kafka_py1.png)


#### 2.3. Run consumer

```
python consumer.py
```
The ```producer.py``` file is:
```
from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from ride import Ride
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


class JsonConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for message_key, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.1',
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])

```
We should get this:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/kafka_py2.png)


We can see running topin on UI  Confluent Control Center (http://localhost:9021/):

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/kafka_py4.png)


- [Avro Producer-Consumer Example](avro_example) using `confluent-kafka` library

Both of these examples require, up-and running Kafka services, therefore please ensure
following steps under [docker-README](docker/README.md)


### 3. Pyspark Structured Streaming

Make sure that all steps 1.1 - 1.3. already done. If not - bulid images and start docker.

#### 3.2. Run producer

```
python producer.py
```
The ```producer.py``` file is:
```
import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, PRODUCE_TOPIC_RIDES_CSV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[9]}, {row[16]}')
                ride_keys.append(str(row[0]))
                i += 1
                if i == 5:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    print(ride_records)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_CSV, records=ride_records)

```
Should get this:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/kafka_py6.png)
