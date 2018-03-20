from flask import Flask, request
import json
from flask import jsonify
from flask_cors import CORS, cross_origin
import uuid
from mc_wrapper import MC_Wrapper

# Using Flask server of python for now
app = Flask(__name__)
CORS(app)

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

if __name__ == "__main__":
    global properties
    global mc

    properties = json.load(open('config.json'))
    mc = MC_Wrapper(properties)

    app.run(debug=True, port=8080, host="0.0.0.0")
