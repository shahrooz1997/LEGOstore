import json
import threading
import socket

from flask import Flask, request
from flask import jsonify
from flask_cors import CORS, cross_origin
from mc_wrapper import MCWrapper

# Using Flask server of python for now
app = Flask(__name__)
CORS(app)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# To bind it with any available configurable IP
sock.bind(('0.0.0.0', 10000))
# Max can handles 1024 connections at one time, can increase it if required
sock.listen(1024)

# Global variable for the mc_wrapper
mc = None


@app.route('/get/<key>', methods=['GET'], strict_slashes=False)
def get_data(key):
    global mc
    print("serving get request ...\n")
    output = mc.get(key)
    return json.dumps({key: output})


@app.route('/put', methods=['PUT'], strict_slashes=False)
def put_data():
    global mc
    print("serving put request ...\n")
    print(type(request.data), type(request.get_data()), type(request.get_json()))
    data = json.loads(request.data.decode('utf-8'))

    output = mc.put(data['key'], data['value'])
    return json.dumps({"updated": output})


class CustomWebException(Exception):

    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        rv['status'] = self.status_code
        return rv


@app.errorhandler(CustomWebException)
def handle_custom_exception(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def server_connection(connection, address):
        # We have kept the data limit to 1000 bytes max, we can increase it if required
    data = connection.recv(64000)
    if not data:
        connection.send(bytes("Error: No data Found"))

    try:
        data = json.dumps(data)
    except:
        print("ERROR: Corrupt data found")

    method = data["method"]
    if method == "get":
        connection.send(bytes(mc.internal_get(data["key"], data["value"])))
    elif method == "put":
        connection.send(bytes(mc.internal_put(data["key"], data["value"], data["dimension"])))
    else:
        connection.send(bytes("MethodNotFound: Unknown method is called"))
    return


def handler():
    while True:
        connection, address = sock.accept()
        cthread = threading.Thread(target=server_connection, agrs=(connection, address))
        cthread.deamon = True
        cthread.start()


if __name__ == "__main__":
    global properties
    global mc

    properties = json.load(open('config.json'))
    mc = MCWrapper(properties)

    thread = threading.Thread(target=handler)
    thread.deamon = True
    thread.start()

    app.run(debug=True, port=8080, host="0.0.0.0")
