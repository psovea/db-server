import time
from flask import Flask, request
import json
from insert_server import PromInsertServer
from mysql_helper import MysqlConnector

app = Flask(__name__)

server = PromInsertServer()

@app.route('/insert-metrics', methods=['POST'])
def insert_metrics():
    data = request.get_json()
    print("Got data: " + str(data))
    for data_point in data:
        meta = data_point['meta']
        metrics = data_point['metrics']
        for metric_name, value in metrics.items():
            # TODO: Let API send metric type (gauge? counter?) and process
            # the metric type in this function.
            server.insert_into_prom(metric_name, value, meta)
    return "Successfully inserted metrics into PrometheusDB."

@app.route('/insert-static', methods=['POST'])
def insert_static():
    sql = MysqlConnector()
    data = request.get_json()
    # print("Got data: " + str(data))
    for operator_name, operator in data.items():
        operator_id = sql.getOrInsert(
            'operators',
            {'name': operator_name},
            {'name': operator_name})
        for line_code, line in operator.items():
            transport_type = line['transportType']
            direction = line['direction']
            transport_type_id = sql.getOrInsert(
                'transport_types',
                {'name': transport_type},
                {'name': transport_type})
            transport_line_id = sql.getOrInsert(
                'transport_lines',
                {'external_code': line_code, 'transport_type_id': transport_type_id, 'direction': direction},
                {'external_code': line_code, 'transport_type_id': transport_type_id, 'direction': direction})
            total_stops = line['totalStops']
            for stop in line['stops']:
                stop_code = stop['stopCode']
                order_number = stop['orderNumber']
                sql.getOrInsert('stops',
                {'external_id': stop_code, 'transport_line_id': transport_line_id, 'order_number': order_number},
                {'external_id': stop_code, 'transport_line_id': transport_line_id, 'order_number': order_number})
    return "Successfully inserted data into MySQL."
