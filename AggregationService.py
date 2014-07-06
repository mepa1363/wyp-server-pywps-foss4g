import ast
import collections
import json
import socket
import urllib2

import psycopg2

from bottle import route, run, request
import config

connection_string = config.conf()

import bottle

bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024


def check_no_of_poi(poi_list, poi_type):
    min_no_of_poi = {'ATM': 1, 'Business': 5, 'Grocery': 1, 'Restaurant': 10, 'Bar': 10, 'Shopping': 5, 'Cinema': 1,
                     'Park': 1, 'Sport': 1, 'Hospital': 1, 'School': 1, 'Library': 1, 'Museum': 1, 'Pharmacy': 1,
                     'Bookstore': 1}

    poi_requirement = {'ATM': True, 'Business': True, 'Grocery': True, 'Restaurant': True, 'Bar': True,
                       'Shopping': True, 'Cinema': True,
                       'Park': True, 'Sport': True, 'Hospital': True, 'School': True, 'Library': True,
                       'Museum': True,
                       'Pharmacy': True,
                       'Bookstore': True}
    #poi_list can be empty when it is created for the first time using the main walkshed
    if poi_list:
        poi_list = ast.literal_eval(str(poi_list))
    full_weight_item_no = 0

    #a poi_type can b
    if poi_type in poi_list:
        if len(poi_list[poi_type]) >= min_no_of_poi[poi_type]:
            for item in poi_list[poi_type]:
                poi_name = item[0]
                poi_weight = float(item[1])
                if poi_weight == 1:
                    full_weight_item_no += 1

    if full_weight_item_no >= min_no_of_poi[poi_type]:
        poi_requirement[poi_type] = False

    return poi_requirement[poi_type]


#prepare WKT polygon for SQL query
def get_wkt_polygon(walkshed):
    polygonJSON = json.loads(walkshed)
    main_walkshed_wkt = "'POLYGON(("
    if 'geometry' in polygonJSON:
        for point in polygonJSON['geometry']['coordinates'][0]:
            longitude = point[0]
            latitude = point[1]
            vertex = "%s %s" % (longitude, latitude)
            main_walkshed_wkt += "%s," % (vertex,)

    else:
        for point in polygonJSON['coordinates'][0]:
            longitude = point[0]
            latitude = point[1]
            vertex = "%s %s" % (longitude, latitude)
            main_walkshed_wkt += "%s," % (vertex,)
    main_walkshed_wkt = main_walkshed_wkt[:-1]
    main_walkshed_wkt += "))'"
    return main_walkshed_wkt


def distance_decay_for_main_walkshed(start_point, main_walkshed, poi_data, walking_time_period):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    #first turning point in seconds
    x1 = 300
    y1 = 1
    #second turning point in seconds
    x2 = int(walking_time_period) * 60
    y2 = 0
    #in order to get rid of division by zero when x1=x2
    #the point is when x2<x1 (threshold) then all the weights will be 1
    if x1 == x2:
        x2 -= 1
        #linear equation
    m = (y2 - y1) / float(x2 - x1)
    b = y2 - m * x2

    main_walkshed_wkt = get_wkt_polygon(main_walkshed)

    poi_data_json = json.loads(poi_data)
    ref_poi_list = {}
    for item in poi_data_json['features']:
        poi_type = str(item['properties']['type'])
        poi_name = str(item['properties']['name'])
        poi_location = item['geometry']['coordinates']
        longitude = poi_location[0]
        latitude = poi_location[1]

        #check if the requirement is met (true/false)
        _isContinued = check_no_of_poi(ref_poi_list, poi_type)

        if _isContinued:
            poi_location_wkt = "'POINT(%s %s)'" % (longitude, latitude)
            within_query = "SELECT ST_Within(ST_Transform(ST_GeomFromText(%s,4326),3776), ST_Transform(ST_GeomFromText(%s,4326),3776));" % (
                poi_location_wkt, main_walkshed_wkt)
            cur.execute(within_query)
            rows = cur.fetchall()
            isWithin = str(rows[0][0])
            if isWithin == 'True':
                otp_url = "http://www.gisciencegroup.ucalgary.ca:8080/opentripplanner-api-webapp/ws/plan?arriveBy=false&time=6%3A58pm&ui_date=1%2F10%2F2013&mode=WALK&optimize=QUICK&maxWalkDistance=5000&walkSpeed=1.38&date=2013-01-10&"
                location = '%s,%s' % (poi_location[1], poi_location[0])
                otp_url += "&toPlace=%s&fromPlace=%s" % (location, start_point)
                otp_data = urllib2.urlopen(otp_url).read()
                otp_data = json.loads(otp_data)
                #time distance between start point and other POIs in seconds
                if otp_data['plan']:
                    time_distance = otp_data['plan']['itineraries'][0]['walkTime']
                    #print type, time_distance
                    if time_distance < x1:
                        weight = 1
                    if (time_distance > x1) and (time_distance < x2):
                        weight = m * time_distance + b
                    if time_distance > x2:
                        weight = 0
                else:
                    weight = 0
            else:
                weight = 0
            name_weight = '%s,%s' % (poi_name, weight)
            name_weight = name_weight.split(',')
            if poi_type in ref_poi_list:
                ref_poi_list[poi_type].append(name_weight)
            else:
                ref_poi_list[poi_type] = ''.split()
                ref_poi_list[poi_type].append(name_weight)

    return ref_poi_list


def distance_decay_for_others(start_point, poi_data, min_time, max_time, ref_poi_list, walkshed):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    poi_data_json = json.loads(poi_data)
    for item in poi_data_json['features']:
        poi_location = item['geometry']['coordinates']
        poi_type = str(item['properties']['type'])
        poi_name = str(item['properties']['name'])
        longitude = poi_location[0]
        latitude = poi_location[1]

        #check if the requirement is met (true/false)
        _isContinued = check_no_of_poi(ref_poi_list, poi_type)

        if _isContinued:
            poi_location_otp = '%s,%s' % (latitude, longitude)
            walkshed_wkt = get_wkt_polygon(walkshed)
            poi_location_wkt = "'POINT(%s %s)'" % (longitude, latitude)
            within_query = "SELECT ST_Within(ST_Transform(ST_GeomFromText(%s,4326),3776), ST_Transform(ST_GeomFromText(%s,4326),3776));" % (
                poi_location_wkt, walkshed_wkt)
            cur.execute(within_query)
            rows = cur.fetchall()
            isWithin = str(rows[0][0])
            if isWithin == 'True':
                otp_url = "http://www.gisciencegroup.ucalgary.ca:8080/opentripplanner-api-webapp/ws/plan?arriveBy=false&time=6%3A58pm&ui_date=1%2F10%2F2013&mode=WALK&optimize=QUICK&maxWalkDistance=5000&walkSpeed=1.38&date=2013-01-10&"
                otp_url += "&toPlace=%s&fromPlace=%s" % (poi_location_otp, start_point)
                try:
                    otp_data = urllib2.urlopen(otp_url).read()
                    otp_data = json.loads(otp_data)
                except urllib2.URLError, e:
                    print e.reason
                except socket.timeout:
                    print "Timed out!"
                    #time distance between start point and other POIs in seconds
                if otp_data['plan']:
                    time_distance = otp_data['plan']['itineraries'][0]['walkTime']
                    if max_time > min_time:
                        #first turning point in seconds
                        x1 = int(min_time)
                        y1 = 1
                        #second turning point in seconds
                        x2 = int(max_time)
                        y2 = 0
                        #in order to get rid of division by zero when x1=x2
                        #the point is when x2<x1 (threshold) then all the weights will be 1
                        if x1 == x2:
                            x2 -= 1
                            #linear equation
                        m = (y2 - y1) / float(x2 - x1)
                        b = y2 - m * x2
                        if time_distance < x1:
                            weight = 1
                        if (time_distance > x1) and (time_distance < x2):
                            weight = m * time_distance + b
                        if time_distance > x2:
                            weight = 0
                    if max_time <= min_time:
                        if time_distance <= max_time:
                            weight = 1
                        else:
                            weight = 0
                else:
                    weight = 0

            else:
                weight = 0
                #compare new weight with reference poi list ()
            for poi in ref_poi_list[poi_type]:
                if poi_name in poi:
                    _weight = float(poi[1])
                    if weight > _weight:
                        poi.pop(1)
                        poi.append(str(weight))
    return ref_poi_list


def make_final_poi_list(start_point, walkshed_collection, poi_data, transit_data, walking_time_period):
    walkshed_collection_json = json.loads(walkshed_collection)
    if 'features' in walkshed_collection_json:
        for walkshed in walkshed_collection_json['features']:
            if walkshed['properties']['id'] == 'main_walkshed':
                ref_poi_list = distance_decay_for_main_walkshed(start_point, json.dumps(walkshed), poi_data,
                                                                walking_time_period)
    else:
        ref_poi_list = distance_decay_for_main_walkshed(start_point, json.dumps(walkshed_collection_json), poi_data,
                                                        walking_time_period)
    if transit_data != '"NULL"':
        transit_data = json.loads(transit_data)
        bus_stops = transit_data['features']
        j = len(bus_stops)
        for i in xrange(len(bus_stops)):
            print j
            location = bus_stops[i]['geometry']['coordinates']
            location = '%s,%s' % (location[1], location[0])
            walking_time = bus_stops[i]['properties']['walking_time_period']
            stop_code = bus_stops[i]['properties']['stop_id']
            for walkshed in walkshed_collection_json['features']:
                if walkshed['properties']['id'] == stop_code:
                    stop_walkshed = json.dumps(walkshed)
                    min_time = 300
                    max_time = int(float(walking_time) * 60)
                    ref_poi_list = distance_decay_for_others(location, poi_data, min_time, max_time, ref_poi_list,
                                                             stop_walkshed)
            j -= 1

    #generate an appropriate poi list for aggregation
    poi_list_for_aggregation = {}
    for poi in ref_poi_list:
        value_list = []
        for item in ref_poi_list[poi]:
            value = item[1]
            value_list.append(float(value))
        value_list.sort(reverse=True)
        poi_list_for_aggregation[poi] = value_list

    return poi_list_for_aggregation


def aggregation(start_point, poi_data, transit_data, crime_data, walkshed_collection, walkshed_union,
                distance_decay_function, walking_time_period):
    if poi_data != '"NULL"':
        poi_weights = {"ATM": [1], "Business": [.5, .45, .4, .35, .3], "Grocery": [3],
                       "Restaurant": [.75, .45, .25, .25, .225, .225, .225, .225, .2, .2],
                       "Bar": [.75, .45, .25, .25, .225, .225, .225, .225, .2, .2],
                       "Shopping": [.5, .45, .4, .35, .3],
                       "Cinema": [1], "Park": [1], "Sport": [1], "Hospital": [1], "School": [1],
                       "Library": [1], "Museum": [1],
                       "Pharmacy": [.5, .45, .4, .35, .3],
                       "Bookstore": [1]}

        #calculate the sum of weights for poi
        poi_sum = 0
        for i in xrange(len(poi_weights)):
            poi_weights_number = poi_weights.items()[i][1]
            for j in xrange(len(poi_weights_number)):
                poi_sum += poi_weights_number[j]

        #calculate poi score
        poi_index = 0
        if distance_decay_function == 'true':
            poi_list = make_final_poi_list(start_point, walkshed_collection, poi_data, transit_data,
                                           walking_time_period)

            for i in xrange(len(poi_list)):
                #poi type
                poi_list_type = poi_list.items()[i][0]
                #the number of POIs in each type
                poi_list_number = poi_list.items()[i][1]
                #the number of weights for each POI type
                poi_weights_number = len(poi_weights[poi_list_type])
                if len(poi_list_number) <= poi_weights_number:
                    for j in xrange(len(poi_list_number)):
                        poi_index += poi_weights[poi_list_type][j] * poi_list_number[j]
                else:
                    for j in xrange(len(poi_weights[poi_list_type])):
                        poi_index += poi_weights[poi_list_type][j] * poi_list_number[j]

        elif distance_decay_function == 'false':
            #list of POIs for the main walkshed
            poi_list = dataPreparation(poi_data)
            poi_list = ast.literal_eval(poi_list)
            for i in xrange(len(poi_list)):
                poi_item_type = poi_list.items()[i][0]
                poi_item_number = poi_list.items()[i][1]
                poi_item_weight_number = len(poi_weights[poi_item_type])
                if poi_item_weight_number >= poi_item_number:
                    for j in xrange(poi_item_number):
                        poi_index += poi_weights[poi_item_type][j]
                else:
                    for j in xrange(poi_item_weight_number):
                        poi_index += poi_weights[poi_item_type][j]
                        #calculate normalized poi score (percentage)
        poi_index_normal = round(poi_index / poi_sum * 100)
    else:
        poi_index_normal = 0

    if crime_data != '"NULL"':
        crime_list = dataPreparation(crime_data)
        crime_list = ast.literal_eval(crime_list)

        crime_weights = {"Arson": [1, 1, 1, 1], "Assault": [10], "Attempted Murder": [4.5, 4.5],
                         "Commercial Break-In": [.5, .5, .5, .5, .5, .5, .5, .5, .5, .5],
                         "Homicide": [9], "Residential Break-In": [.5, .5, .5, .5, .5, .5, .5, .5, .5, .5],
                         "Robbery": [2, 1.5, 1.5], "Sex Offence": [10],
                         "Theft": [.4, .4, .4, .4, .4, .4, .4, .4, .4, .4],
                         "Theft From Vehicle": [.3, .3, .3, .3, .3, .3, .3, .3, .3, .3],
                         "Vandalism": [.2, .2, .2, .2, .2, .2, .2, .2, .2, .2],
                         "Vehicle Theft": [.1, .1, .1, .1, .1, .1, .1, .1, .1, .1]}

        #calculate the sum of weights for crime
        crime_sum = 0
        for i in xrange(len(crime_weights)):
            crime_weights_number = crime_weights.items()[i][1]
            for j in xrange(len(crime_weights_number)):
                crime_sum += crime_weights_number[j]

        #calculate crime score
        crime_index = 0
        for i in xrange(len(crime_list)):
            crime_item_type = crime_list.items()[i][0]
            crime_item_number = crime_list.items()[i][1]
            crime_item_weight_number = len(crime_weights[crime_item_type])
            if crime_item_weight_number >= crime_item_number:
                for i in xrange(crime_item_number):
                    crime_index += crime_weights[crime_item_type][i]
            else:
                for i in xrange(crime_item_weight_number):
                    crime_index += crime_weights[crime_item_type][i]

        #calculate normalized crime score (percentage) for the main walkshed
        crime_index_normal = round(crime_index / crime_sum * 100)
    else:
        crime_index_normal = 0

    if (crime_index_normal >= 0) and (crime_index_normal < 20):
        crime_color_hex = '#39B54A'
    elif (crime_index_normal >= 20) and (crime_index_normal < 40):
        crime_color_hex = '#8DC63F'
    elif (crime_index_normal >= 40) and (crime_index_normal < 60):
        crime_color_hex = '#FFF200'
    elif (crime_index_normal >= 60) and (crime_index_normal < 80):
        crime_color_hex = '#F7941E'
    elif (crime_index_normal >= 80) and (crime_index_normal <= 100):
        crime_color_hex = '#ED1C24'

    #calculate area of the walkshed
    area = calculateArea(walkshed_union)
    polygonJSON = json.loads(walkshed_union)
    if 'features' in polygonJSON:
        final_walkshed = '{"type": "FeatureCollection", "features": ['
        for feature in polygonJSON['features']:
            _type = feature['type']
            _geometry = feature['geometry']
            _geometry = json.dumps(_geometry)
            final_walkshed += '{"type": "%s", "geometry": %s, "properties": {"type": "Walkshed", "area": %s, "score": "%d", "crime_index": %s, "color": "%s"}},' % (
                _type, _geometry, area, poi_index_normal, crime_index_normal, crime_color_hex)
        final_walkshed = final_walkshed[:-1]
        final_walkshed += ']}'
    else:
        final_walkshed = walkshed_union[:-1]
        final_walkshed += ',"properties": {"type": "Walkshed", "area": %s, "score": "%d", "crime_index": %s, "color": "%s"}}' % (
            area, poi_index_normal, crime_index_normal, crime_color_hex)
    return final_walkshed


def dataPreparation(data):
    data_json = json.loads(data)
    data_type = []
    for item in data_json['features']:
        data_type.append(str(item['properties']['type']))
    data_type_counter = collections.Counter(data_type)
    data_for_aggregation = str(data_type_counter)[8:-1]
    return data_for_aggregation


def calculateArea(walkshed):
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    polygonJSON = json.loads(walkshed)
    area = []
    #if the walkshed is multipolygon
    if 'features' in polygonJSON:
        for polygon in polygonJSON['features']:
            finalPolygon = "POLYGON(("
            for item in polygon['geometry']['coordinates']:
                for point in item:
                    longitude = point[0]
                    latitude = point[1]
                    vertex = "%s %s" % (longitude, latitude)
                    finalPolygon += "%s," % (vertex,)
                finalPolygon = finalPolygon[:-1]
                finalPolygon += "))"
                select_query = "SELECT ST_Area(ST_Transform(ST_GeomFromText(%s, 4326), 3776));"
                parameters = [finalPolygon]
                cur.execute(select_query, parameters)
                rows = cur.fetchone()
                area.append(rows[0])
    else:
        finalPolygon = "POLYGON(("
        for item in polygonJSON['coordinates']:
            for point in item:
                longitude = point[0]
                latitude = point[1]
                vertex = "%s %s" % (longitude, latitude)
                finalPolygon += "%s," % (vertex,)
        finalPolygon = finalPolygon[:-1]
        finalPolygon += "))"
        select_query = "SELECT ST_Area(ST_Transform(ST_GeomFromText(%s, 4326), 3776));"
        parameters = [finalPolygon]
        cur.execute(select_query, parameters)
        rows = cur.fetchone()
        area.append(rows[0])

    conn.commit()
    cur.close()
    conn.close()
    return sum(area)


@route('/aggregation', method='POST')
def service():
    poi_data = request.POST.get('poi', default=None)
    crime_data = request.POST.get('crime', default=None)
    walkshed_collection = request.POST.get('walkshed_collection', default=None)
    walkshed_union = request.POST.get('walkshed_union', default=None)
    start_point = request.POST.get('start_point', default=None)
    transit_data = request.POST.get('transit', default=None)
    distance_decay_function = request.POST.get('distance_decay_function', default=None).lower()
    walking_time_period = request.POST.get('walking_time_period', default=None)

    if start_point and poi_data and crime_data and walkshed_collection and walkshed_union and transit_data and distance_decay_function and walking_time_period is not None:
        return aggregation(start_point, poi_data, transit_data, crime_data, walkshed_collection, walkshed_union,
                           distance_decay_function, walking_time_period)


run(host='0.0.0.0', port=9364, debug=True)
