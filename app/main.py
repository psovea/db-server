import time
from flask import Flask, request
import json
from insert_server import PromInsertServer
from mysql_helper import MysqlConnector

app = Flask(__name__)

server = PromInsertServer()

def make_transport_line(operator_id, dir_id, line_id, transport_type_id, line_obj):
    """Returns an object for a transport line to be placed in the db"""
    return {"operator_id": operator_id,
            "direction_id": dir_id,
            "internal_id": line_id,
            "public_id": line_obj["lineNumberName"],
            "name": line_obj["lineName"],
            "destination_name": line_obj["destinationName"],
            "direction": line_obj["direction"],
            "transport_type_id": transport_type_id,
            "total_stops": line_obj["totalStops"]}


def make_stop(stop_id, lat, lon, name, town, area_code, access_wc, access_vi):
    """ Creates a stop object to be placed in the database """
    return {"stop_code": stop_id,
            "lat": lat,
            "lon": lon,
            "name": name,
            "town": town,
            "area_code": area_code,
            "accessibility_wheelchair": access_wc,
            "accessibility_visual": access_vi}


def make_transport_line_stop(transport_line_id, stop_id, order):
    """Returns a transport line stop object to be placed in the database"""
    return {"transport_line_id": transport_line_id,
            "stop_id": stop_id,
            "order_number": order}


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

        # database expects these values to be 1 or 0
        access_wc = 1 if stop["accessibility"]["wheelchair"] else 0
        access_vi = 1 if stop["accessibility"]["visual"] else 0

        insert_obj = make_stop(stop_id, lat, lon, name, town, area_code,
                access_wc, access_vi)
        
        print("insert_obj ", insert_obj)
        try:
            sql.getOrInsert("stops", insert_obj, insert_obj)
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
            transport_type_id = sql.getId("transport_types",
                    {"name": line_obj["transportType"]})
            internal_id = line_obj["lineCode"]

            transport_line_obj = make_transport_line(operator_id, line_id, internal_id,
                    transport_type_id, line_obj)

            try:
                sql.getOrInsert("transport_lines", transport_line_obj,
                        transport_line_obj)

                # connect every stop with a line id
                for stop in line_obj["stops"]:
                    stop_id = sql.getId("stops",
                            {"stop_code": stop["stopCode"]})
                    

                    transport_line_id = sql.getId("transport_lines",
                            {"direction_id": line_id, "operator_id": operator_id})
                    order = stop["orderNumber"]

                    transport_line_stop = make_transport_line_stop(
                            transport_line_id,
                            stop_id, order)

                    sql.getOrInsert("transport_lines_stops",
                            transport_line_stop, transport_line_stop)
            except Exception as e:
                print(e)
    return "Successfully inserted data into MySQL."

@app.route('/get-line-mapping', methods=['GET'])
def get_line_mapping():
    sql = MysqlConnector()
    dicts = {a:b for a,b in sql.getLineToTypeMapping()}
    return json.dumps(dicts)
