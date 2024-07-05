from flask import Flask, request
from producer import produce_message

app = Flask(__name__)

@app.route('/send', methods=['POST'])
def send_message():
    message = request.json.get('message')
    produce_message(message)
    return 'Message sent!', 200

if __name__ == '__main__':
    app.run(port=5001)