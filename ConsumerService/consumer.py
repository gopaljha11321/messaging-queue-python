from confluent_kafka import Consumer, KafkaException, KafkaError

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "group1",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def consume_messages():
    consumer.subscribe(['my_topic'])
    try:
        while True:
            # Increase the timeout value here (e.g., to 5 seconds)
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            print(f'Received message: {msg.value().decode("utf-8")}')
    except KafkaException as e:
        print(f"Kafka exception: {e}")
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        consumer.close()