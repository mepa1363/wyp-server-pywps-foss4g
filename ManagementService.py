import json
import urllib2
from xml.dom.minidom import parseString

from bottle import route, run, request


def parseXML(string_xml, element):
    xml_dom = parseString(string_xml)
    node_list = xml_dom.getElementsByTagName(element)[0].childNodes
    result = ''
    for node in node_list:
        if node.nodeType == node.TEXT_NODE:
            result = node.data
    return result


def management(start_point, start_time, walking_time_period, walking_speed, bus_waiting_time, bus_ride_time,
               distance_decay_function):
    pywps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi?service=WPS&version=1.0.0&request=execute&identifier='

    #invoke walkshed service to get the main walkshed
    otp_dataInputs = 'StartPoint=%s;WalkingPeriod=%s;WalkingSpeed=%s;WalkshedOutput=SHED' % (
        start_point, walking_time_period, walking_speed)
    otp_url = pywps_url + 'Walkshed_Transit_Centralized&datainputs=' + otp_dataInputs
    walkshed_main = urllib2.urlopen(otp_url).read()
    walkshed_main = parseXML(walkshed_main, 'wps:LiteralData')

    if walkshed_main != '':
        walkshed_json = json.loads(walkshed_main)
        if walkshed_json['type'] == "Polygon":

            print 'main walkshed: %s' % (walkshed_main,)

            #invoke transit service to get bus stops
            #start_time = '16:10:00'
            transit_wps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi'
            transit_xml_data = """<wps:Execute xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ows="http://www.opengis.net/ows/1.1" service="WPS" version="1.0.0" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd">
    <ows:Identifier>Transit_Centralized</ows:Identifier>
    <wps:DataInputs>
        <wps:Input>
            <ows:Identifier>StartTime</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>Walkshed</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>WalkingTime</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>WalkingSpeed</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>BusWaitingTime</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>BusRideTime</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
    </wps:DataInputs>
</wps:Execute>""" % (start_time, walkshed_main, walking_time_period, walking_speed, bus_waiting_time, bus_ride_time)
            transit_wps_request = urllib2.Request(url=transit_wps_url, data=transit_xml_data,
                                                  headers={'Content-Type': 'application/xml'})

            transit_data = urllib2.urlopen(transit_wps_request).read()
            transit_data = parseXML(transit_data, 'wps:LiteralData')

            print 'transit data: %s' % (transit_data,)

            if transit_data != '"NULL"':
                #invoke walkshed service to get walkshed collection
                transit_data_json = json.loads(transit_data)
                walkshed_collection = '{"type": "FeatureCollection", "features": ['
                for item in transit_data_json['features']:
                    longitude = item['geometry']['coordinates'][0]
                    latitude = item['geometry']['coordinates'][1]
                    start_point = "%s,%s" % (latitude, longitude)
                    walkTime = item['properties']['walking_time_period']
                    stop_code = item['properties']['stop_id']
                    otp_dataInputs = 'StartPoint=%s;WalkingPeriod=%s;WalkingSpeed=%s;WalkshedOutput=SHED' % (
                        start_point, walkTime, walking_speed)
                    otp_url = pywps_url + 'Walkshed_Transit_Centralized&datainputs=' + otp_dataInputs
                    otp_data = urllib2.urlopen(otp_url).read()
                    otp_data = parseXML(otp_data, 'wps:LiteralData')
                    otp_data = json.loads(otp_data)
                    if otp_data['type'] == "Polygon":
                        otp_data = json.dumps(otp_data)
                        walkshed_collection += '{"type": "Feature","geometry": %s, "properties": {"id": %s}},' % (
                            otp_data, stop_code)
                walkshed_collection += '{"type": "Feature","geometry": %s, "properties": {"id": "main_walkshed"}}' % (
                    walkshed_main,)
                walkshed_collection += ']}'

                print 'walkshed collection: %s' % (walkshed_collection,)

                #invoke union service
                union_wps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi'
                union_xml_data = """<wps:Execute xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ows="http://www.opengis.net/ows/1.1" service="WPS" version="1.0.0" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd">
    <ows:Identifier>Union_Centralized</ows:Identifier>
    <wps:DataInputs>
        <wps:Input>
            <ows:Identifier>WalkshedCollection</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
    </wps:DataInputs>
</wps:Execute>""" % (walkshed_collection,)
                union_wps_request = urllib2.Request(url=union_wps_url, data=union_xml_data,
                                                    headers={'Content-Type': 'application/xml'})
                walkshed_union = urllib2.urlopen(union_wps_request).read()
                walkshed_union = parseXML(walkshed_union, 'wps:LiteralData')
            #print walkshed_union

            else:
                walkshed_union = walkshed_main
                walkshed_collection = walkshed_main

            print 'walkshed union: %s' % (walkshed_union,)

            #invoke poi service
            poi_wps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi'
            poi_xml_data = """<wps:Execute xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ows="http://www.opengis.net/ows/1.1" service="WPS" version="1.0.0" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd">
    <ows:Identifier>POI_Transit_Centralized</ows:Identifier>
    <wps:DataInputs>
        <wps:Input>
            <ows:Identifier>Walkshed</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
    </wps:DataInputs>
</wps:Execute>""" % (walkshed_union,)
            poi_wps_request = urllib2.Request(url=poi_wps_url, data=poi_xml_data,
                                              headers={'Content-Type': 'application/xml'})
            poi_data = urllib2.urlopen(poi_wps_request).read()
            poi_data = parseXML(poi_data, 'wps:LiteralData')

            poi_data = poi_data.replace("'", "")

            print 'poi data: %s' % (poi_data,)

            #invoke crime wps
            crime_wps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi'
            crime_xml_data = """<wps:Execute xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ows="http://www.opengis.net/ows/1.1" service="WPS" version="1.0.0" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd">
    <ows:Identifier>Crime_Transit_Centralized</ows:Identifier>
    <wps:DataInputs>
        <wps:Input>
            <ows:Identifier>Walkshed</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
    </wps:DataInputs>
</wps:Execute>""" % (walkshed_union,)
            crime_wps_request = urllib2.Request(url=crime_wps_url, data=crime_xml_data,
                                                headers={'Content-Type': 'application/xml'})
            crime_data = urllib2.urlopen(crime_wps_request).read()
            crime_data = parseXML(crime_data, 'wps:LiteralData')

            print "crime data: %s" % (crime_data,)

            #retrieve aggregated data (final result)
            #As the volume of input data is too large for HTTP GET request, HTTP POST is used to invoke GeoServer Aggregation WPS

            aggregation_wps_url = 'http://127.0.0.1/cgi-bin/pywps.cgi'
            aggregation_xml_data = """<wps:Execute xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ows="http://www.opengis.net/ows/1.1" service="WPS" version="1.0.0" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd">
    <ows:Identifier>Aggregation_Transit_Centralized</ows:Identifier>
    <wps:DataInputs>
        <wps:Input>
            <ows:Identifier>POI</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>Crime</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>Transit</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>StartPoint</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>WalkshedCollection</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>WalkshedUnion</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>DistanceDecayFunction</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
        <wps:Input>
            <ows:Identifier>WalkingTimePeriod</ows:Identifier>
            <wps:Data>
                <wps:LiteralData dataType="string">%s</wps:LiteralData>
            </wps:Data>
        </wps:Input>
    </wps:DataInputs>
</wps:Execute>""" % (
                poi_data, crime_data, transit_data, start_point, walkshed_collection, walkshed_union,
                distance_decay_function,
                walking_time_period)

            aggregation_wps_request = urllib2.Request(url=aggregation_wps_url, data=aggregation_xml_data,
                                                      headers={'Content-Type': 'application/xml'})

            aggregation_data = urllib2.urlopen(aggregation_wps_request).read()
            aggregation_data = parseXML(aggregation_data, 'wps:LiteralData')

            print 'aggregation data: %s' % (aggregation_data,)

        else:
            aggregation_data = '"NULL"'
            poi_data = '"NULL"'
            crime_data = '"NULL"'
            transit_data = '"NULL"'
    else:
        aggregation_data = '"NULL"'
        poi_data = '"NULL"'
        crime_data = '"NULL"'
        transit_data = '"NULL"'
    return aggregation_data
    #result = '{"walkshed": %s, "poi": %s, "crime": %s, "transit": %s}' % (
    #   aggregation_data, poi_data, crime_data, transit_data)
    return result


@route('/management')
def service():
    start_point = request.GET.get('start_point', default=None)
    start_time = request.GET.get('start_time', default=None)
    walking_time_period = request.GET.get('walking_time_period', default=None)
    walking_speed = request.GET.get('walking_speed', default=None)
    bus_waiting_time = request.GET.get('bus_waiting_time', default=None)
    bus_ride_time = request.GET.get('bus_ride_time', default=None)
    distance_decay_function = request.GET.get('distance_decay_function', default=None)

    if start_point and start_time and walking_time_period and walking_speed and bus_waiting_time and bus_ride_time and distance_decay_function is not None:
        return management(start_point, start_time, walking_time_period, walking_speed, bus_waiting_time, bus_ride_time,
                          distance_decay_function)


run(host='0.0.0.0', port=9363, debug=True)

#http://127.0.0.1:9363/management?start_point=51.048981,-114.03153&start_time=16:00:00&walking_time_period=15&walking_speed=1.38&bus_waiting_time=10&bus_ride_time=5&distance_decay_function=false
