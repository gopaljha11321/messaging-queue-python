from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_message(message):
    try:
        producer.produce('my_topic', message.encode('utf-8'), callback=delivery_report)
        producer.poll(1)
    except Exception as e:
        print(f"Failed to produce message: {e}")