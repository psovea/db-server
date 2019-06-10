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

@app.route('/insert-static-stops', methods=['POST'])
def insert_static_stops():
    sql = MysqlConnector()
    data = request.get_json()

    stop_ids = data.keys()

    for stop_id in stop_ids:
        stop = data[stop_id]
        lat = stop["lat"]
        lon = stop["lon"]
        name = stop["name"]
        town = stop["town"]
        area_code = stop["areaCode"]
        accessibility_wheelchair = 1 if stop["accessibility"]["wheelchair"] else 0
        accessibility_visual = 1 if stop["accessibility"]["visual"] else 0 
        
        try:
            sql.getOrInsert("stops", {"stop_code": stop_id, "lat": lat, "lon": lon, "name": name, "town": town, "area_code": area_code, "accessibility_wheelchair": accessibility_wheelchair, "accessibility_visual":accessibility_visual}, {"stop_code": stop_id, "lat": lat, "lon": lon, "name": name, "town": town, "area_code": area_code, "accessibility_wheelchair": accessibility_wheelchair, "accessibility_visual":accessibility_visual}) 
        except Exception as e:
            print(e)
            print(stop)

        return "Great success."

@app.route('/insert-static', methods=['POST'])
def insert_static():
    sql = MysqlConnector()
    data = request.get_json()

    operators = data.keys()

    for operator in operators:
        sql.getOrInsert("operators", {"name": operator}, {"name": operator})
        
        operator_id = sql.getId("operators", {"name": operator})
    
        for line_id in data[operator].keys():
            line_obj = data[operator][line_id]
            transport_type_id = sql.getId("transport_types", {"name": line_obj["transportType"]})

            try: 
                sql.getOrInsert("transport_lines", {"operator_id": operator_id, "internal_id": line_id, "public_id": line_obj["lineNumberName"], "name": line_obj["lineName"], "destination_name": line_obj["destinationName"], "direction": line_obj["direction"], "transport_type_id": transport_type_id, "total_stops": line_obj["totalStops"]}, {"operator_id": operator_id, "internal_id": line_id, "public_id": line_obj["lineNumberName"], "name": line_obj["lineName"], "destination_name": line_obj["destinationName"], "direction": line_obj["direction"], "transport_type_id": transport_type_id, "total_stops": line_obj["totalStops"]})
                    
                for stop in line_obj["stops"]:
                    stop_id = sql.getId("stops", {"stop_code": stop["stopCode"]})
                    transport_line_id = sql.getId("transport_lines", {"internal_id": line_id})
                    order = stop["orderNumber"]
                    sql.getOrInsert("transport_lines_stops", {"transport_line_id": transport_line_id, "stop_id": stop_id, "order_number": order}, {"transport_line_id": transport_line_id, "stop_id": stop_id, "order_number": order})
            except Exception as e:
                print(e)
#    for operator_name, operator in data.items():
#        operator_id = sql.getOrInsert(
#            'operators',
#            {'name': operator_name},
#            {'name': operator_name})
#        for line_code, line in operator.items():
#            transport_type = line['transportType']
#            direction = line['direction']
#            transport_type_id = sql.getOrInsert(
#                'transport_types',
#                {'name': transport_type},
#                {'name': transport_type})
#            transport_line_id = sql.getOrInsert(
#                'transport_lines',
#                {'external_code': line_code, 'transport_type_id': transport_type_id, 'direction': direction},
#                {'external_code': line_code, 'transport_type_id': transport_type_id, 'direction': direction})
#            total_stops = line['totalStops']
#            for stop in line['stops']:
#                stop_code = stop['stopCode']
#                order_number = stop['orderNumber']
#                sql.getOrInsert('line_stops',
#                {'external_id': stop_code, 'transport_line_id': transport_line_id, 'order_number': order_number},
#                {'external_id': stop_code, 'transport_line_id': transport_line_id, 'order_number': order_number})
    return "Successfully inserted data into MySQL."

@app.route('/get-line-mapping', methods=['GET'])
def get_line_mapping():
    sql = MysqlConnector()
    dicts = {a:b for a,b in sql.getLineToTypeMapping()}
    return json.dumps(dicts)
