import json

import requests
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

STOP_URL = "http://18.216.203.6:5000/get-stops"
GEOJSON_URL = "http://184.72.120.43:3000/districts"


# def fromRdToWgs(coords):
#     """
#     Weird hacky function to convert rijksdriehoekscoordinaten to GPS coordinates
#     """
#     X0 = 155000
#     Y0 = 463000
#     phi0 = 52.15517440
#     lam0 = 5.38720621
#     Kp = [0, 2, 0, 2, 0, 2, 1, 4, 2, 4, 1]
#     Kq = [1, 0, 2, 1, 3, 2, 0, 0, 3, 1, 1]
#     Kpq = [3235.65389, -32.58297, -0.24750, -0.84978, -0.06550, -
#            0.01709, -0.00738, 0.00530, -0.00039, 0.00033, -0.00012]

#     Lp = [1, 1, 1, 3, 1, 3, 0, 3, 1, 0, 2, 5]
#     Lq = [0, 1, 2, 0, 3, 1, 1, 2, 4, 2, 0, 0]
#     Lpq = [5260.52916, 105.94684, 2.45656, -0.81885, 0.05594, -
#            0.05607, 0.01199, -0.00256, 0.00128, 0.00022, -0.00022, 0.00026]

#     dX = 1E-5 * (coords[0] - X0)
#     dY = 1E-5 * (coords[1] - Y0)

#     phi = 0
#     lam = 0

#     for k in range(len(Kpq)):
#         phi = phi + (Kpq[k] * dX**Kp[k] * dY**Kq[k])
#     phi = phi0 + phi / 3600

#     for l in range(len(Lpq)):
#         lam = lam + (Lpq[l] * dX**Lp[l] * dY**Lq[l])
#     lam = lam0 + lam / 3600

#     return [phi, lam]


def bind_district():
    """
    Function which reads stop data and district coordinate data
    """
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
    return json.dumps(district_data)
