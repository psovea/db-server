import json
import zmq
import xmltodict
import gzip
import requests

ARRIVAL = "ARRIVAL"
OPERATOR = "GVB"


punctualities = {}
counters = {}
line_info = {}
stop_info = {}


def line_URL(line, operator):
    """ URL for retrieving information regarding a vehicle type from the database """
    return "http://18.216.203.6:5000/get-lines?public_id={}&operator={}".format(
        line,
        operator)


def stop_URL(stop_code):
    """ URL for retrieving information regarding a stop from the database """
    return "http://18.216.203.6:5000/get-stops?stop_code={}".format(stop_code)


def filter_arrivals(tp, obj):
    """ Filter arrivals on type and line number """
    return list(obj.keys())[0] == tp  # and obj[tp]['lineplanningnumber'] == line_num


def get_line_info(line):
    URL = line_URL(line, OPERATOR)
    try:
        data = requests.get(url=URL).json()
    except requests.exceptions.RequestException:
        data = []
    return None if data == [] else data[0]


def get_stop_info(stop_code):
    URL = stop_URL("300" + stop_code)
    try:
        data = requests.get(url=URL).json()
    except requests.exceptions.RequestException:
        data = []
    return None if data == [] else data[0]


def location_punctuality_metric(begin, end, increase, vehicle_number, line_number):
    district = None

    try:
        district = stop_info[end]['district']
    except KeyError:
        stop = get_stop_info(end)
        if stop is not None:
            stop_info[end] = stop
            district = stop_info[end]['district']
        else:
            return {}

    return {
        'metrics': {
            'location_punctuality': increase
        },
        'meta': {
            'stop_begin': '300' + begin['stop_code'],
            'stop_end': '300' + end,
            'transport_type': begin['transport_type'],
            'district': district,
            'operator': begin['operator'],
            'line_number': line_number
        }
    }


def parse_message(message):
    pos_info = None

    try:
        data = message['VV_TM_PUSH']['KV6posinfo']
        pos_info = [data] if type(data) is dict else data
    except KeyError as e:
        print(e)
        return []

    arrivals = [el for el in pos_info if filter_arrivals(ARRIVAL, el)]

    # print(arrivals)

    for obj in arrivals:
        obj = obj[ARRIVAL]
        stop = obj['userstopcode']
        punc = int(obj['punctuality'])
        line_num = obj['lineplanningnumber']
        vehicle_num = obj['vehiclenumber']

        try:
            prev = punctualities[obj['vehiclenumber']]
            increase = punc - prev['punctuality']

            met = location_punctuality_metric(
                prev, stop, increase, vehicle_num, line_num)
            key = tuple(met['meta'].items())
            if not key in counters:
                counters[key] = 0
            if increase > 0:
                counters[key] += increase

            punctualities[obj['vehiclenumber']]['punctuality'] = punc
            punctualities[obj['vehiclenumber']]['stop_code'] = stop

        except KeyError:
            print('added {} to punctualities'.format(obj['vehiclenumber']))

            line_info = get_line_info(line_num)

            if line_info is None:
                continue

            punctualities[obj['vehiclenumber']] = {
                'punctuality': int(punc),
                'stop_code': stop,
                'transport_type': line_info['transport_type'],
                'operator': line_info['operator']
            }
