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
###
@app.route('/', methods=['POST'])
def main():
    data = json.loads(request.get_json())
    for data_point in data:
        meta = data_points['meta']
        metrics = data_points['metrics']
        for metric_name, value in metrics:
            ## TODO: Let API send metric type (gauge? counter?) and process
            ## the metric type in this function.
            server.insert_into_prom(metric_name, value, meta)

server = PromInsertServer()
