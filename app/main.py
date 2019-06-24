import time
from flask import Flask, request, send_from_directory, jsonify
from mysql_helper import MysqlConnector, build_query
import bind_stop_code_to_district
from datetime import datetime

import sys
import os
# sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")
# import analytics.app as analytics
sys.path.insert(0, 'home/ubuntu/analytics/app')
import fetch_prometheus as fp

import json
import requests

REMOTEPROMINSERT = False

app = Flask(__name__)

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
        "stop_name": name,
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

if REMOTEPROMINSERT:
    from insert_server import PromInsertServer
    server = PromInsertServer()

    @app.route('/insert-metrics', methods=['POST'])
    def insert_metrics():
        """Insert transport metrics into the Prometheus DB."""
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

@app.route('/match-districts-with-stops', methods=['GET'])
def bind_stops_to_districts():
    """Takes stop-code to district mappings from bind_stop_code_to_district.py
    and inserts them into the database."""
    sql = MysqlConnector()
    bindings = bind_stop_code_to_district.get_stop_to_district_binds()
    for data in bindings:
        district_id = sql.getOrInsert('districts', {'name': data['district']}, {'name': data['district']})
        query = "UPDATE stops SET district_id = {} WHERE stop_code = '{}'".format(district_id, data['stop_code'])
        sql.execQuery(query, no_result=True)
        print("{} -> {}".format(data['stop_code'], data['district']))
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
    """Get the information for one or more spetop_ten_bottleneckscific lines, like the route
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


def delay_filters(data):
    """Get the label filters from the given data dict in the right form"""
    districts = data.getlist('district[]')
    transport_types = data.getlist('transport_type[]')
    operators = data.getlist('operator[]')
    stop_begins = data.getlist('stop_begin[]')
    stop_ends = data.getlist('stop_end[]')
    line_numbers = data.getlist('line_number[]')

    labels = {
        'district': districts,
        'transport_type': transport_types,
        'operator': operators,
        'stop_begin': stop_begins,
        'stop_end': stop_ends,
        'line_number': line_numbers
    }

    return {key: '|'.join(value) for key, value in labels.items()}

def recent_period_func(func, metric, labels, period):
    """Make a json query for prometheus which applies a func over a recent period,
    e.g the last x days"""
    return [{
        func: {
            "metric": metric,
            "labels": labels,
            "period": period
        }
    }]

def specific_period_func(func, metric, labels, data):
    """Make a json query for prometheus which applies a func over a specific period,
    e.g every tuesday from 4 to 7 PM or yesterday the whole day"""
    start_day_time = data.get('start_time', 0, type=int)
    end_day_time = data.get('end_time', 86400, type=int)
    valid_days = data.getlist('valid_days[]')
    past_days = data.get('past_days', 0, type=int)

    now = datetime.now()
    seconds_since_midnight = int((now - now.replace(hour=0, minute=0, second=0,
                                            microsecond=0)).total_seconds())

    offset = seconds_since_midnight - end_day_time
    time_range = end_day_time - start_day_time
    today = datetime.today().weekday()

    inner_query = lambda off_time: {
        func: {
            "metric": metric,
            "labels": labels,
            "period": str(time_range) + "s",
            "offset": str(off_time) + "s"
        }
    }

    days_to_find = []
    for day in range(past_days + 1):
        if offset > 0 and (not valid_days or (today - day) % 7 in valid_days):
            days_to_find.append(inner_query(offset))
        offset += 86400
    return days_to_find

def heatmap_format(query_result, metric_stop):
    """Reformat the query_result to get it in heatmap format
    metric_stop specifies which stop from the metric is used for location"""
    url = "http://18.224.29.151:5000/get-stops?town=amsterdam"
    r = requests.get(url)
    stops_json = r.json()
    stops = {stop['stop_code']: (stop['lat'], stop['lon']) for stop in stops_json}
    # Get the largest value of query_result to use it for normalizing
    max_val = max(query_result, key=lambda x:x['value'][1])
    return [[*stops[point['metric'][metric_stop]], (float(point['value'][1]) / max_val)] for point in query_result]

@app.route('/get_delays', methods=['GET'])
def get_delays():
    """Get the delays according to GET arguments; How to exactly use this endpoint,
    see the analytics wiki."""

    data = request.args
    labels = delay_filters(data)
    return_filters = data.getlist('return_filter[]')

    if 'period' in data:
        main_query = recent_period_func("increase", "location_punctuality",
                                          labels, data.get('period', type=str))
    else:
        main_query = specific_period_func("increase", "location_punctuality",
                                            labels, data)
    sample = {
        "sum": {
            "+": main_query
        },
        "by": return_filters
    }
    if 'top' in data:
        sample = {
            'topk': {
                'k': data.get('top'),
                'subquery': sample
            }
        }

    query_result = fp.execute_json_prom_query(sample)

    if 'format' in data:
        if data['format'] == 'heatmap':
            query_result = heatmap_format(query_result, 'stop_end')
    return jsonify(query_result)
