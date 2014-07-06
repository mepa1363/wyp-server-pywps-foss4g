import json
import urllib2
from bottle import route, run, request

def getPolygon(polygon):
    polygonJSON = json.loads(polygon)
    polygon_collection = []
    if 'features' in polygonJSON:
        for feature in polygonJSON['features']:
            for obj in feature['geometry']['coordinates']:
                new_polygon = ''
                for point in obj:
                    longitude = point[0]
                    latitude = point[1]
                    new_polygon += '<latLng><lat>%s</lat><lng>%s</lng></latLng>' % (latitude, longitude)
                polygon_collection.append(new_polygon)
    else:
        new_polygon = ''
        for point in polygonJSON['coordinates'][0]:
            longitude = point[0]
            latitude = point[1]
            new_polygon += '<latLng><lat>%s</lat><lng>%s</lng></latLng>' % (latitude, longitude)
        polygon_collection.append(new_polygon)
    return polygon_collection


def getPOIs(walkshed):
    app_key = 'Fmjtd%7Cluuan90bnh%2C8w%3Do5-96r5l4'
    polygon_collection = getPolygon(walkshed)
    poi_type_list = {"ATM": '3002', "Business": '3011', "Grocery": '3012', "Restaurant": '3016', "Bar": '3017',
                     "Shopping": '3020',
                     "Cinema": '3029', "Park": '3034', "Sport": '3040', "Hospital": '3043', "School": '3045',
                     "Library": '3046', "Museum": '3047',
                     "Pharmacy": '3053',
                     "Bookstore": '3054'}
    poi_type = ''
    for item in poi_type_list:
        poi_type += "T='" + poi_type_list[item] + "' OR "
    poi_type = poi_type[:-4]
    hosted_data = 'MQA.NTPois'
    information_field = 'T,Phone,Address'
    max_matches = '10000'
    outFormat = 'json' #xml

    result_json = '{"type": "FeatureCollection", "features": ['
    for polygon in polygon_collection:
        poi_url = "http://www.mapquestapi.com/search/v1/polygon?key=" + app_key + "&inFormat=xml&outFormat=" + outFormat
        poi_xml_data = """
    <search>
        <polygon>
            %s
        </polygon>
        <options>
            <maxMatches>%s</maxMatches>
        </options>
        <hostedDataList>
            <hostedData>
                <name>%s</name>
                <extraCriteria>%s</extraCriteria>
                <fields>
                    <field>T</field>
                    <field>Phone</field>
                    <field>Address</field>
                </fields>
            </hostedData>
        </hostedDataList>
    </search>""" % (polygon, max_matches, hosted_data, poi_type)
        poi_wps_request = urllib2.Request(url=poi_url, data=poi_xml_data,
            headers={'Content-Type': 'application/xml'})
        poi_data = urllib2.urlopen(poi_wps_request).read()
        poi_json = json.loads(poi_data)
        if(poi_json['resultsCount']) > 0:
            result_count = len(poi_json['searchResults'])
            for i in xrange(result_count):
                poi_data_name = poi_json['searchResults'][i]['name']
                poi_data_name = poi_data_name.replace('&', 'and')
                poi_data_type = poi_json['searchResults'][i]['fields']['T']
                for item in poi_type_list.items():
                    if item[1] == poi_data_type:
                        poi_data_type = item[0]
                poi_data_icon = poi_json['searchResults'][i]['poiImageUrl']
                poi_data_lat = poi_json['searchResults'][i]['shapePoints'][0]
                poi_data_lon = poi_json['searchResults'][i]['shapePoints'][1]
                poi_data_phone = poi_json['searchResults'][i]['fields']['Phone']
                poi_data_address = poi_json['searchResults'][i]['fields']['Address']
                location = "[%s,%s]" % (poi_data_lon, poi_data_lat)
                result_json += '{"type": "Feature","geometry": {"type": "Point", "coordinates":%s}, "properties": {"name": "%s", "type": "%s", "icon": "%s", "phone": "%s", "address": "%s"}},' % (
                    location, poi_data_name, poi_data_type, poi_data_icon, poi_data_phone, poi_data_address)
    if result_json == '{"type": "FeatureCollection", "features": [':
        result_json = '"NULL"'
    else:
        result_json = result_json[:-1]
        result_json += ']}'
    return result_json


@route('/poi')
def service():
    polygon = request.GET.get('walkshed', default=None)
    if polygon is not None:
        return getPOIs(polygon)

run(host='0.0.0.0', port=9365, debug=True)

#54.213.109.58:9365/poi?walkshed={"type":"Polygon","coordinates":[[[-114.13258446221629,51.05667765949581],[-114.13254056321318,51.05780123264723],[-114.13152943754814,51.05928339714371],[-114.12950549567188,51.06011065488163],[-114.1279654,51.0595932],[-114.12445710928647,51.062350863156894],[-114.12371101357336,51.063263785101434],[-114.12310669501387,51.06399487923505],[-114.12123542899363,51.0661290239349],[-114.11806424260443,51.06644301813297],[-114.11786253384714,51.066370448767316],[-114.1164274,51.063358],[-114.1134139,51.0622939],[-114.1131162,51.0622073],[-114.1129392,51.0620646],[-114.11184,51.0609932],[-114.1073205,51.0596571],[-114.1063016,51.060466691883235],[-114.10383914444445,51.06000902380952],[-114.10321092026163,51.05965823334367],[-114.10018115215432,51.057739378598626],[-114.09998273312733,51.05760439367275],[-114.1038694944397,51.05455092810189],[-114.10400678562115,51.054463627570605],[-114.10475924522562,51.05399662522794],[-114.10556177053202,51.053499575688136],[-114.10861280557579,51.05160815940013],[-114.11152012383799,51.049742240547495],[-114.1129164,51.0497427],[-114.11648368491431,51.04959693181472],[-114.11835347765114,51.04940523337369],[-114.1215325,51.0523811],[-114.1221979,51.0527387],[-114.122947,51.053085],[-114.12355379949769,51.05334646431694],[-114.125129,51.053914],[-114.128853,51.055076],[-114.129204,51.055165],[-114.1308835,51.0556508],[-114.13194309123665,51.05595505984363],[-114.13258446221629,51.05667765949581]]]}
