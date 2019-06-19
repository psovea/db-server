import json
import zmq
import xmltodict
import gzip
import requests


ARRIVAL = "ARRIVAL"
DEPARTURE = "DEPARTURE"

OPERATOR = "GVB"

CONTEXT = zmq.Context()
SOCKET = CONTEXT.socket(zmq.SUB)

url = 'tcp://pubsub.besteffort.ndovloket.nl:7658'

SOCKET.connect(url)

topicfilter = "/GVB/"
SOCKET.setsockopt_string(zmq.SUBSCRIBE, topicfilter)

punctualities = {}
counters = {}
line_info = {}
stop_info = {}

prefix_dict = {"GVB": "300"}


def line_URL(line, operator):
    """ URL for retrieving information regarding a vehicle type from the database """
    return "http://18.224.29.151:5000/get-lines?public_id={}&operator={}".format(
        line,
        operator)


def stop_URL(stop_code):
    """ URL for retrieving information regarding a stop from the database """
    return "http://18.224.29.151:5000/get-stops?stop_code={}".format(stop_code)


def filter_messages(tp, obj):
    """ Filter arrivals on type and line number """
    return list(obj.keys())[0] == tp  # and obj[tp]['lineplanningnumber'] == line_num


def get_line_info(line):
    URL = line_URL(line, OPERATOR)
    try:
        data = requests.get(url=URL).json()
    except requests.exceptions.RequestException:
        print("Cannot get line info from db")
        data = []
    return None if data == [] else data[0]


def get_stop_info(stop_code):
    URL = stop_URL(stop_code)
    try:
        data = requests.get(url=URL).json()
    except requests.exceptions.RequestException:
        print("Cannot get stop info from db")
        data = []
    return None if data == [] else data[0]


def location_punctuality_metric(begin, end, increase, vehicle_number, line_number):
    """Create a location punctuality metric from the given variables"""
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
            'stop_begin': begin['stop_code'],
            'stop_end': end,
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
        return []

    arrivals = [el for el in pos_info if filter_messages(ARRIVAL, el)]
    departures = [el for el in pos_info if filter_messages(DEPARTURE, el)]

    for obj in arrivals:
        obj = obj[ARRIVAL]
        punc = int(obj['punctuality'])
        line_num = obj['lineplanningnumber']
        vehicle_num = obj['vehiclenumber']

        prefix = None
        try:
            prefix = prefix_dict[obj['dataownercode']]
        except KeyError:
            prefix = ""
        stop = prefix + obj['userstopcode']

        try:
            if punctualities[obj['vehiclenumber']]['arrived']:
                continue
            punctualities[obj['vehiclenumber']]['arrived'] = True
            prev = punctualities[obj['vehiclenumber']]
            increase = punc - prev['punctuality']
            met = location_punctuality_metric(
                prev, stop, increase, vehicle_num, line_num)
            key = tuple(met['meta'].items())
            if not key in counters:
                counters[key] = 0
            if increase > 0:
                counters[key] += increase
        except KeyError:
            continue

    for obj in departures:
        obj = obj[DEPARTURE]
        punc = int(obj['punctuality'])
        line_num = obj['lineplanningnumber']
        vehicle_num = obj['vehiclenumber']

        prefix = None
        try:
            prefix = prefix_dict[obj['dataownercode']]
        except KeyError:
            prefix = ""
        stop = prefix + obj['userstopcode']

        if line_num not in line_info:
            line_info[line_num] = get_line_info(line_num)

        if line_info[line_num] is None:
            continue

        try:
            punctualities[obj['vehiclenumber']]['punctuality'] = punc
            punctualities[obj['vehiclenumber']]['stop_code'] = stop
            punctualities[obj['vehiclenumber']]['arrived'] = False

        except KeyError:
            punctualities[obj['vehiclenumber']] = {
                'punctuality': int(punc),
                'stop_code': stop,
                'transport_type': line_info[line_num]['transport_type'],
                'operator': line_info[line_num]['operator'],
                'arrived': False
            }


def ordered_dict_to_dict(od):
    """ hacky way to create regular dict from ordered dict. """
    return json.loads(json.dumps(od))


if __name__ == '__main__':
    from insert_server import PromInsertServer
    server = PromInsertServer(8001, counters)
    while True:
        message = SOCKET.recv_multipart()
        xml = gzip.decompress(message[1]).decode("utf-8")
        message = ordered_dict_to_dict(xmltodict.parse(xml))
        parse_message(message)
