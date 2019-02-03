import requests
import time
import json
import polyline

class OSRM:
    def __init__(self, request_base_uri="http://router.project-osrm.org/", 
        route_export_path="data/route.txt"):

        if  "http://" not in request_base_uri and "https://" not in request_base_uri:
            raise Exception("Not a valid URL! Url must contain 'http' or 'https'")

        self.route_request_string = request_base_uri + "route/v1/driving/{0},{1};{2},{3}?overview=full&steps=true&annotations=speed"
        self.distance_request_string = request_base_uri + "route/v1/driving/{0},{1};{2},{3}?overview=simplified"

    def getFullRoute(self, start, destination):
        print("Getting route...")
        url_request = self.route_request_string.format(start.Longitude, start.Latitude, destination.Longitude, destination.Latitude)
        return self.getResponse(url_request)

    def getSimpleRoute(self, start, destination):
        print("Getting route...")
        url_request = self.route_request_string.format(start.Longitude, start.Latitude, destination.Longitude, destination.Latitude)
        return self.getResponse(url_request)

    def getElevationFromSegment(self, start, end):
        return 0

    def decodePolyline(self, line):
        return polyline.decode(line)

    def getResponse(self, request_url):
        for _ in range(3):
            try:
                response = requests.get(request_url)
                break
            except:
                time.sleep(1)
        
        content = response.content

        if "Too many requests" in content:
            raise Exception("Unable to perform API request, try again later.")

        return json.loads(content.decode("utf8").replace("'",'"'))

        