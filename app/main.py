import time
from flask import Flask, request, send_from_directory, jsonify
import json
# from insert_server import PromInsertServer
from mysql_helper import MysqlConnector, build_query
import bind_stop_code_to_district

import json
import zmq
import xmltodict
import gzip
import requests


app = Flask(__name__)

# server = PromInsertServer()

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


def get_transport_line(tup):
    """Returns an object for a transport line to send through the endpoint"""
    operator, int_id, pub_id, name, dest, direction, trans_type, stops = tup
    return {
        "direction": direction,
        "public_id": pub_id,
        "internal_id": int_id,
        "transport_type": trans_type,
        "line_name": name,
        "operator": operator,
        "destination": dest,
        "num_stops": stops
    }


def make_stop(stop_id, lat, lon, name, town, area_code, access_wc, access_vi):
    """Returns a stop object to be placed in the database"""
    return {
        "stop_code": stop_id,
        "lat": lat,
        "lon": lon,
        "stop_name": name,
        "town": town,
        "area_code": area_code,
        "accessibility_wheelchair": access_wc,
        "accessibility_visual": access_vi
    }


def get_stop(tup):
    """Returns a stop object to send through the endpoint"""
    stop_id, lat, lon, name, town, area_code, access_wc, access_vi, district = tup
    return {
        "stop_code": stop_id,
        "lat": lat,
        "lon": lon,
        "name": name,
        "town": town,
        "area_code": area_code,
        "access": {
            "wheelchair": access_wc,
            "visual": access_vi
        },
        "district": district
    }


def make_transport_line_stop(transport_line_id, stop_id, order):
    """Returns a transport line stop object to be placed in the database"""
    return {
        "transport_line_id": transport_line_id,
        "stop_id": stop_id,
        "order_number": order
    }


def get_transport_line_stop(tup):
    """Returns a transport line stop object to send through the endpoint"""
    stop_code, stop_name, order_number, int_id, direction = tup
    return {
        "stop_code": stop_code,
        "stop_name": stop_name,
        "order_number": order_number,
        "internal_id": int_id,
        "direction": direction
    }

@app.route('/', methods=['GET'])
def test():
    return 'success!'

# @app.route('/insert-metrics', methods=['POST'])
# def insert_metrics():
#     """Insert transport metrics into the Prometheus DB."""
#     data = request.get_json()
#     print("Got data: " + str(data))
#     for data_point in data:
#         meta = data_point['meta']
#         metrics = data_point['metrics']
#         for metric_name, value in metrics.items():
#             # TODO: Let API send metric type (gauge? counter?) and process
#             # the metric type in this function.
#             server.insert_into_prom(metric_name, value, meta)
#     return "Successfully inserted metrics into PrometheusDB."

@app.route('/get-heatmap-info', methods=['GET'])
def get_heatmap_info():
    import sys
    sys.path.insert(0, '/home/ubuntu/analytics/app')
    import get_graph
    data = request.args
    transport_type = data.get("transport_type", default=None)
    operator = data.get("operator", default=None)
    district = data.get("district", default=None)
    period = data.get("period", default="d")
    return jsonify(get_graph.get_coor_weight_json(period, transport_type, operator, district))

@app.route('/get-district-delays', methods=['GET'])
def get_district_delays():
    import sys
    sys.path.insert(0, '/home/ubuntu/analytics/app')
    import fetch_prometheus
    data = request.args
    amount = data.get("amount", default=None)
    unit = data.get("unit", default='d')
    return jsonify(fetch_prometheus.donut_districts(amount=amount, unit=unit))

@app.route('/insert-static-stops', methods=['POST'])
def insert_static_stops():
    """Instert all stops into the MySQL DB."""
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
    """Insert all lines and corresponding data into the MySQL DB."""
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

# Takes stop-code to district mappings from bind_stop_code_to_district.py
# and inserts them into the database.
@app.route('/match-districts-with-stops', methods=['GET'])
def bind_stops_to_districts():
    print("Finding all district mappings...")
    sql = MysqlConnector()
    bindings = bind_stop_code_to_district.get_stop_to_district_binds()
    print("Starting inserting into database.")
    for data in bindings:
        district_id = sql.getOrInsert('districts', {'name': data['district']}, {'name': data['district']})
        query = "UPDATE stops SET district_id = {} WHERE stop_code = '{}'".format(district_id, data['stop_code'])
        sql.execQuery(query, no_result=True)
        print("{} -> {}".format(data['stop_code'], data['district']))
    print("Done!")
    return "Done!"

@app.route('/get-line-mapping', methods=['GET'])
def get_line_mapping():
    sql = MysqlConnector()
    dicts = {a: b for a, b in sql.getLineToTypeMapping()}
    return json.dumps(dicts)

@app.route('/get-stops', methods=['GET'])
def get_stops():
    """Get stops from the MySQL DB."""

    stop_code = request.args.get("stop_code", default="%", type=str).split(",")
    lat = request.args.get("lat", default="%", type=str).split(",")
    lon = request.args.get("lon", default="%", type=str).split(",")
    # TODO: handle stop names with spaces in them (aka almost all of them).
    name = request.args.get("name", default="%", type=str).split(",")
    town = request.args.get("town", default="%", type=str).split(",")
    area_code = request.args.get("area_code", default="%", type=str).split(",")
    wheelchair_accs = request.args.get("wheelchair", default="%", type=bool)
    visual_accs = request.args.get("visual", default="%", type=bool)
    district = request.args.get("district", default="%", type=str).split(",")

    sql = MysqlConnector()

    keys = ["stop_code", "lat", "lon", "stops.name", "town", "area_code", "accessibility_wheelchair", "accessibility_visual", "districts.name"]

    search_values = {
        "stop_code": stop_code,
        "lat": lat,
        "lon": lon,
        "stops.name": name,
        "town": town,
        "area_code": area_code,
        "accessibility_wheelchair": wheelchair_accs,
        "accessibility_visual": visual_accs,
        "districts.name": district
    }

    join = {
        "districts": ("districts.id", "stops.district_id")
    }

    query = build_query('stops', keys, search_values, join)

    print("got request")

    return json.dumps([get_stop(stop) for stop in sql.execQuery(query)]), {'Content-Type': 'application/json'}


@app.route('/get-lines', methods=['GET'])
def get_lines():
    """Get lines from the MySQL DB."""
    operator = request.args.get("operator", default="%", type=str).split(",")
    internal_id = request.args.get(
        "internal_id", default="%", type=str).split(",")
    public_id = request.args.get("public_id", default="%", type=str).split(",")
    # TODO: handle destination names with spaces.
    name = request.args.get("name", default="%", type=str).split(",")
    destination_name = request.args.get(
        "destination", default="%", type=str).split(",")
    direction = request.args.get("direction", default="1", type=int)
    transport_type = request.args.get(
        "transport_type", default="%", type=str).split(",")
    total_stops = request.args.get("total_stops", default="%", type=int)

    sql = MysqlConnector()

    search_values = {
        "operators.name": operator,
        "internal_id": internal_id,
        "public_id": public_id,
        "transport_lines.name": name,
        "destination_name": destination_name,
        "direction": direction,
        "transport_types.name": transport_type,
        "total_stops": total_stops
    }

    join = {
        "operators": ("transport_lines.operator_id", "operators.id"),
        "transport_types": ("transport_lines.transport_type_id", "transport_types.id")
    }

    query = build_query("transport_lines", search_values.keys(),
                        search_values, inner_join=join)
    return json.dumps([get_transport_line(line) for line in sql.execQuery(query)]), {"Content-Type": "application/json"}


@app.route('/get-line-info', methods=['GET'])
def get_line_info():
    """Get the information for one or more specific lines, like the route
       that line takes from the MySQL DB.
    """
    internal_ids = request.args.get(
        "internal_id", default="%", type=str).split(",")
    operator = request.args.get("operator", default="%", type=str).split(",")
    direction = request.args.get("direction", default="%", type=str).split(",")

    sql = MysqlConnector()
    line_info_list = []
    keys = ["stops.stop_code", "stops.name",
            "transport_lines_stops.order_number", "transport_lines.internal_id", "transport_lines.direction"]

    search_values = {
            "transport_lines.internal_id": internal_ids,
            "operators.name": operator,
            "transport_lines.direction": direction
        }

    join = {
            "transport_lines_stops": ("stops.id", "transport_lines_stops.stop_id"),
            "transport_lines": ("transport_lines_stops.transport_line_id", "transport_lines.id"),
            "operators": ("transport_lines.operator_id", "operators.id")
        }

    order = ["transport_lines.internal_id",
             "transport_lines.direction",
             "transport_lines_stops.order_number"]

    query = build_query("stops", keys, search_values, join,
                        order)
    line_info = [get_transport_line_stop(line_stop)
                           for line_stop in sql.execQuery(query)]

    prev_order_id = 0

    for i, line_stop in enumerate(line_info):
        if line_stop["order_number"] <= line_info[prev_order_id]["order_number"]:
            line_info_list.append(line_info[prev_order_id:i])
            prev_order_id = i

    # The first list is empty, so we should omit that (hacky I know)
    return json.dumps(line_info_list[1:]), {"Content-Type": "application/json"}



@app.route('/get-districts', methods=['GET'])
def get_districts():
    return send_from_directory("../static", "districts.geojson"), {'Content-Type': 'application/json'}


@app.route('/get_delays', methods=['GET'])
def get_delays():
    import sys
    sys.path.insert(0, '/home/ubuntu/analytics/app')
    import top_ten_bottlenecks
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

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=5000)
