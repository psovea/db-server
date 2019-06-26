import json

import requests
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

STOP_URL = "http://18.216.203.6:5000/get-stops"
GEOJSON_URL = "http://184.72.120.43:3000/districts"


def get_stop_to_district_binds():
    """Function which reads stop data and district coordinate data"""
    r1 = requests.get(STOP_URL)
    stops = r1.json()

    r2 = requests.get(GEOJSON_URL)
    geojson = r2.json()

    # Make a list of city districts with their polygon coordinates
    districts = []
    for district in geojson['features']:
        name = district['properties']['STADSDEELN']
        coordinates = district['geometry']['coordinates']
        districts.append((name, coordinates))

    # For each stop, calculate in which district the stop lies
    district_data = []
    for stop in stops:
        coord = (float(stop['lat']), float(stop['lon']))
        point = Point(*coord)
        for district in districts:
            polygon = Polygon(district[1])
            if polygon.contains(point):
                district_data.append({
                    'stop_code': stop['stop_code'],
                    'district': district[0]
                })
    return district_data
