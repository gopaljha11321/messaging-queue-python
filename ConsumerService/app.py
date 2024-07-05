from flask import Flask
from threading import Thread
from consumer import consume_messages

app = Flask(__name__)

@app.route('/')
def home():
    return 'Service B is running', 200

def start_consumer():
    consumer_thread = Thread(target=consume_messages)
    consumer_thread.start()

if __name__ == '__main__':
    print("Main thread starting...")
    start_consumer()  # Start Kafka consumer in a separate thread
    app.run(port=5000, threaded=True)  # Run Flask app with threaded=True for multi-threading
