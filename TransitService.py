import datetime
import json
import ast

import psycopg2
from bottle import route, run, request

import config


connection_string = config.conf()

#Find all bus stops within walkshed
def getAllBusStops(polygon):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    selectQuery = "SELECT stop_code " \
                  "FROM transit_oct_dec_2012.tblstops " \
                  "WHERE ST_Contains(ST_GeomFromText(%s, 4326), stop_location);"
    parameters = [polygon]
    cur.execute(selectQuery, parameters)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    bus_stops = {}
    for i in xrange(len(rows)):
        bus_stops[i] = rows[i][0]
    return bus_stops

#Find distinct routes through bus stops
def getDistinctRoutes(bus_stops, arrival_time_from, arrival_time_to):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    selectQuery = "SELECT DISTINCT r.route_short_name " \
                  "FROM transit_oct_dec_2012.tblstop_times AS st, transit_oct_dec_2012.tbltrips AS t, transit_oct_dec_2012.tblstops AS s, transit_oct_dec_2012.tblroutes AS r " \
                  "WHERE (st.trip_id = t.trip_id) AND (st.stop_id = s.stop_id) AND (t.route_id = r.route_id) AND (s.stop_code = ANY (ARRAY["
    for i in xrange(len(bus_stops)):
        selectQuery += "'%s'," % (bus_stops[i])
    selectQuery = selectQuery[:-1]
    selectQuery += "])) AND (st.arrival_time BETWEEN %s AND %s) AND (t.service_id = 1) ORDER BY r.route_short_name;"
    parameters = [arrival_time_from, arrival_time_to]
    cur.execute(selectQuery, parameters)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    routes = {}
    for i in xrange(len(rows)):
        routes[i] = rows[i][0]
    return routes

#Find closest bust stop to start point for each route; start point is 51.05723044585338,-114.11717891693115
#Two steps needed: (i) find all stops for each route within the walkshed, (ii) find closest stop to start point (for each route)

#(i) find all stops for each route within the walkshed
def getBusStopsAlongRoute(polygon, routes):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    bus_stops_along_routes = {}
    for i in xrange(len(routes)):
        route = routes[i]
        selectQuery = "SELECT DISTINCT s.stop_code " \
                      "FROM transit_oct_dec_2012.tblstop_times AS st, transit_oct_dec_2012.tbltrips AS t, transit_oct_dec_2012.tblstops AS s, transit_oct_dec_2012.tblroutes AS r " \
                      "WHERE (st.trip_id = t.trip_id) AND (st.stop_id = s.stop_id) AND (t.route_id = r.route_id) AND " \
                      "ST_Contains(ST_GeomFromText(%s, 4326), s.stop_location) AND (r.route_short_name = %s) AND (t.service_id = 1);"
        parameters = [polygon, route]
        cur.execute(selectQuery, parameters)
        rows = cur.fetchall()
        bus_stops = []
        for i in xrange(len(rows)):
            bus_stops.append(rows[i][0])
        bus_stops_along_routes[route] = len(rows), bus_stops
    conn.commit()
    cur.close()
    conn.close()
    return bus_stops_along_routes


#(ii) find closest stop to start point (for each route)
def getClosestStopToStartPoint(all_bus_stops_along_all_routes):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    bus_stops = ast.literal_eval(str(all_bus_stops_along_all_routes))
    closest_bus_stops = {}
    for obj in bus_stops:
        route = obj
        stop_code_list = bus_stops[obj][1]
        selectQuery = "SELECT s.stop_code, ST_Distance(ST_Transform(ST_GeomFromText('POINT (-114.11717891693115 51.05723044585338)',4326),3776), ST_Transform(s.stop_location, 3776)) AS Stop_Distance " \
                      "FROM transit_oct_dec_2012.tblstops AS s " \
                      "WHERE (s.stop_code = ANY (%s)) ORDER BY Stop_Distance LIMIT 1;"
        parameters = [stop_code_list]
        cur.execute(selectQuery, parameters)
        rows = cur.fetchall()
        closest_bus_stops[route] = rows[0]
    conn.commit()
    cur.close()
    conn.close()
    return closest_bus_stops

#find nex bus for the closest bus stop on each route
def getNextBus(closest_bus_stops, start_time_seconds, walking_speed, bus_waiting_time):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    bus_stops = ast.literal_eval(str(closest_bus_stops))
    next_bus = {}
    for obj in bus_stops:
        route = obj
        stop_code = bus_stops[obj][0]
        distance_to_closest_stop = bus_stops[obj][1]
        time_to_closest_stop = round(distance_to_closest_stop / walking_speed)
        arrival_time_from_seconds = start_time_seconds + time_to_closest_stop
        arrival_time_to_seconds = arrival_time_from_seconds + bus_waiting_time * 60

        arrival_time_to = datetime.datetime(1, 1, 1) + datetime.timedelta(seconds=arrival_time_to_seconds)
        arrival_time_to = '%s:%s:%s' % (arrival_time_to.hour, arrival_time_to.minute, arrival_time_to.second)

        arrival_time_from = datetime.datetime(1, 1, 1) + datetime.timedelta(seconds=arrival_time_from_seconds)
        arrival_time_from = '%s:%s:%s' % (arrival_time_from.hour, arrival_time_from.minute, arrival_time_from.second)

        selectQuery = "SELECT r.route_short_name, t.trip_id, st.arrival_time, t.shape_id " \
                      "FROM transit_oct_dec_2012.tblstop_times AS st, transit_oct_dec_2012.tbltrips AS t, transit_oct_dec_2012.tblstops AS s, transit_oct_dec_2012.tblroutes AS r " \
                      "WHERE (st.trip_id = t.trip_id) AND (st.stop_id = s.stop_id) AND (t.route_id = r.route_id) AND " \
                      "(s.stop_code = %s) AND (r.route_short_name = '%s') AND " \
                      "(st.arrival_time BETWEEN %s AND %s) AND (t.service_id = 1) ORDER BY st.arrival_time LIMIT 1;"
        parameters = [stop_code, route, arrival_time_from, arrival_time_to]
        cur.execute(selectQuery, parameters)
        rows = cur.fetchall()
        if len(rows) > 0:
            route_id = rows[0][0]
            trip_id = rows[0][1]
            arrival_time = rows[0][2].strftime('%H:%M:%S')
            shape_id = rows[0][3]
            next_bus[route_id] = trip_id, arrival_time, shape_id
    conn.commit()
    cur.close()
    conn.close()
    return next_bus

#get accessible bus stops
def getAccessibleBusStops(next_bus_list, ride_time):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    bus_stops = ast.literal_eval(str(next_bus_list))
    accessible_stops = {}
    for obj in bus_stops:
        route = obj
        trip_id = bus_stops[obj][0]
        arrival_time = bus_stops[obj][1]
        time_elements = str(arrival_time).split(':')
        arrival_time_seconds = int(time_elements[0]) * 3600 + int(time_elements[1]) * 60 + int(time_elements[2])
        arrival_time_from = arrival_time_seconds - ride_time * 60
        arrival_time_from = str(datetime.timedelta(seconds=arrival_time_from))
        arrival_time_to = arrival_time_seconds + ride_time * 60
        arrival_time_to = str(datetime.timedelta(seconds=arrival_time_to))
        selectQuery = "SELECT st.arrival_time, s.stop_code, s.stop_lat, s.stop_lon, t.shape_id " \
                      "FROM transit_oct_dec_2012.tblstops AS s, transit_oct_dec_2012.tbltrips AS t, transit_oct_dec_2012.tblstop_times AS st, transit_oct_dec_2012.tblroutes AS r " \
                      "WHERE (r.route_id = t.route_id) AND (t.trip_id = st.trip_id) AND (st.stop_id = s.stop_id) AND " \
                      "(r.route_short_name = %s) AND (t.service_id = 1) AND (t.trip_id = %s) " \
                      "AND (st.arrival_time BETWEEN %s AND %s) " \
                      "ORDER BY st.arrival_time;"
        parameters = [route, trip_id, arrival_time_from, arrival_time_to]
        cur.execute(selectQuery, parameters)
        rows = cur.fetchall()
        stop_list = ''
        for i in xrange(len(rows)):
            arrival_time = rows[i][0].strftime('%H:%M:%S')
            stop_code = rows[i][1]
            stop_lat = rows[i][2]
            stop_lon = rows[i][3]
            stop_location = "[%s,%s]" % (stop_lon, stop_lat)
            stop_list += "(%s,%s,'%s')," % (stop_code, stop_location, arrival_time)
        accessible_stops[route] = ast.literal_eval(stop_list)
    conn.commit()
    cur.close()
    conn.close()
    return accessible_stops

#invoke all functions to get the final result which is accessible bus stops
def invoke(polygon, start_time, walking_time_period, walking_speed, bus_waiting_time, bus_ride_time):
    min_walking_time_period = 3
    walking_time_period = int(walking_time_period)
    walking_speed = float(walking_speed)
    bus_waiting_time = int(bus_waiting_time)
    bus_ride_time = int(bus_ride_time)
    polygonJSON = json.loads(polygon)
    polygon = "POLYGON(("
    for point in polygonJSON['coordinates'][0]:
        longitude = point[0]
        latitude = point[1]
        vertex = "%s %s" % (longitude, latitude)
        polygon += "%s," % (vertex,)
    polygon = polygon[:-1]
    polygon += "))"
    #retrive all bus stops within the walkshed
    bus_stops = getAllBusStops(polygon)
    if len(bus_stops) > 0:
        time_elements = str(start_time).split(':')
        start_time_seconds = int(time_elements[0]) * 3600 + int(time_elements[1]) * 60 + int(time_elements[2])
        arrival_time_to_seconds = start_time_seconds + bus_waiting_time * 60
        arrival_time_to = datetime.datetime(1, 1, 1) + datetime.timedelta(seconds=arrival_time_to_seconds)
        arrival_time_to = '%s:%s:%s' % (arrival_time_to.hour, arrival_time_to.minute, arrival_time_to.second)
        #retrieve all distinct routes within the walkshed between stat walking time and start walking time plus bus waiting time
        routes = getDistinctRoutes(bus_stops, start_time, arrival_time_to)
        #retrieve all bus stops along with the routes
        bus_stops_along_routes = getBusStopsAlongRoute(polygon, routes)
        #retrieve closest bus stops on each route to the start point
        closest_stops = getClosestStopToStartPoint(bus_stops_along_routes)
        #retrieve next buses coming according to bus waiting time for the closest stops on each route
        next_bus_list = getNextBus(closest_stops, start_time_seconds, walking_speed, bus_waiting_time)
        #retrieve accessible bus stops
        accessible_stops = getAccessibleBusStops(next_bus_list, bus_ride_time)
        if len(accessible_stops) > 0:
            result_json = '{"type": "FeatureCollection", "features": ['
            for obj in accessible_stops:
                route = obj
                no_of_stops = len(accessible_stops[obj])
                for i in xrange(no_of_stops):
                    stop_code = accessible_stops[obj][i][0]
                    stop_location = accessible_stops[obj][i][1]
                    bus_stop_time = accessible_stops[obj][i][2]
                    time_elements = str(bus_stop_time).split(':')
                    bus_stop_time_seconds = int(time_elements[0]) * 3600 + int(time_elements[1]) * 60 + int(
                        time_elements[2])
                    bus_stop_walking_time_period = start_time_seconds + walking_time_period * 60 - bus_stop_time_seconds #start_time_seconds + walking_time_seconds - bus_stop_time_seconds
                    #bus_stop_walking_time_period = str(datetime.timedelta(seconds=bus_stop_walking_time_period))
                    bus_stop_walking_time_period /= float(60)
                    if bus_stop_walking_time_period < min_walking_time_period:
                        bus_stop_walking_time_period = min_walking_time_period
                    result_json += '{"type": "Feature","geometry": {"type": "Point", "coordinates":%s}, "properties": {"route": %s, "stop_id": %s, "walking_time_period": "%s"}},' % (
                        stop_location, route, stop_code, bus_stop_walking_time_period)
            result_json = result_json[:-1]
            result_json += ']}'
        else:
            result_json = '"NULL"'
    else:
        result_json = '"NULL"'
    return result_json


@route('/transit')
def service():
    walkshed = request.GET.get('walkshed', default=None)
    start_time = request.GET.get('start_time', default=None)
    walking_time_period = request.GET.get('walking_time_period', default=None)
    walking_speed = request.GET.get('walking_speed', default=None)
    bus_waiting_time = request.GET.get('bus_waiting_time', default=None)
    bus_ride_time = request.GET.get('bus_ride_time', default=None)

    if walkshed and start_time and walking_time_period and walking_speed and bus_waiting_time and bus_ride_time is not None:
        return invoke(walkshed, start_time, walking_time_period, walking_speed, bus_waiting_time, bus_ride_time)


run(host='0.0.0.0', port=9367, debug=True)


#http://127.0.0.1:9367/transit?walkshed={"type":"Polygon","coordinates":[[[-114.13365602972974,51.058347391891886],[-114.13357621235956,51.057855987640444],[-114.13344618196722,51.0572335704918],[-114.13306763846154,51.05680758846154],[-114.13234774444444,51.05613223333333],[-114.13225402820512,51.05604434487179],[-114.13164571594783,51.0556029063246],[-114.1309099,51.0554052],[-114.1296983,51.0550805],[-114.12935211425557,51.0549877123115],[-114.12777429230611,51.05456481086652],[-114.1260463,51.0539968],[-114.1254663,51.0538296],[-114.1219747,51.0522907],[-114.11835347765114,51.04940523337369],[-114.11649501480825,51.04959793831729],[-114.1129164,51.0497427],[-114.11155016630754,51.04974225043314],[-114.10862910952382,51.0516081984127],[-114.10475919285714,51.05400655535714],[-114.10400678807339,51.054463860550456],[-114.10387211,51.05455092],[-114.10003730970543,51.05760434981218],[-114.10023477344399,51.0577393439834],[-114.10320235242503,51.05965814022199],[-114.10383903333333,51.06001977142857],[-114.1063016,51.0607218],[-114.1073205,51.0596571],[-114.11184,51.0609932],[-114.1129392,51.0620646],[-114.1131162,51.0622073],[-114.1134139,51.0622939],[-114.11552680517585,51.0632131679058],[-114.11786579555954,51.06712926324684],[-114.121156241444,51.066239255465106],[-114.12311166601941,51.06404440291262],[-114.1247202124067,51.06240921696865],[-114.12493555381388,51.06153181981867],[-114.12945306431409,51.0603580411729],[-114.13181078947369,51.059427273684214],[-114.13365602972974,51.058347391891886]]]}&start_time=16:10:00&walking_time_period=15&walking_speed=1.38&bus_waiting_time=10&bus_ride_time=5
