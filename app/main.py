import time
from flask import Flask, request, jsonify
import json
from insert_server import PromInsertServer
from analytics import top_ten_bottlenecks
from kv6 import parse_message

import json
import zmq
import xmltodict
import gzip
import requests

CONTEXT = zmq.Context()
SOCKET = CONTEXT.socket(zmq.SUB)

url = 'tcp://pubsub.besteffort.ndovloket.nl:7658'

SOCKET.connect(url)

topicfilter = "/GVB/"
SOCKET.setsockopt_string(zmq.SUBSCRIBE, topicfilter)

app = Flask(__name__)

###
# [
#   {
#     metrics: { punctuality: '0' },
#     meta: { operator_name: 'GVB', line_number: '22', vehicle_number: '362' }
#   }
# ]
# [
# {
# 'metrics': {'punctuality': '0'},
# 'meta': {'operator_name': 'GVB', 'line_number': '22', 'vehicle_number': '362'}
# }
# ]
###

server = PromInsertServer()


@app.route('/', methods=['POST'])
def main():
    # print(request.get_json())
    data = request.get_json()
    print("Got data: " + str(data))
    for data_point in data:
        meta = data_point['meta']
        metrics = data_point['metrics']
        for metric_name, value in metrics.items():
            # TODO: Let API send metric type (gauge? counter?) and process
            # the metric type in this function.
            server.insert_into_prom(metric_name, value, meta)
    return "Received!!!"


@app.route('/get_delays', methods=['GET'])
def get_delays():
    data = request.args
    time_begin = data.get('begin', default=0)
    time_end = data.get('end', default=86400)
    valid_days = data.getlist('day[]')
    if not valid_days:
        valid_days = [0, 1, 2, 3, 4, 5, 6]
    period = data.get('period', default=14)
    districts = data.getlist('district[]')
    if not districts:
        districts = ['Oost', 'Zuidoost', 'Noord', 'Westpoort',
                     'West', 'Centrum', 'Nieuw-West', 'Zuid']
    transport_types = data.getlist('transport_type[]')
    if not transport_types:
        transport_types = ['BUS']
    operators = data.getlist('operator[]')
    if not operators:
        operators = ['GVB']
    return_filters = data.getlist('return_filter[]')
    return jsonify(top_ten_bottlenecks(time_begin, time_end, valid_days, period, districts=districts,
                                       transport_types=transport_types, operators=operators, return_filters=return_filters))


def ordered_dict_to_dict(od):
    """ hacky way to create regular dict from ordered dict. """
    return json.loads(json.dumps(od))


while True:
    message = SOCKET.recv_multipart()
    xml = gzip.decompress(message[1]).decode("utf-8")
    message = ordered_dict_to_dict(xmltodict.parse(xml))
    parse_message(message)
