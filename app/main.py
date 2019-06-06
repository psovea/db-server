import time
from flask import Flask, request
import json
from insert_server import PromInsertServer

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
