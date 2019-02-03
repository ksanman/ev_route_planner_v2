from osrm import OSRM
import json
from coordinate import Coordinate

class Route:
    def __init__(self):
        self.routingMachine = OSRM()

    def buildRoute(self, startingCoordinate, endingCoordinate):
        osrmRouteData = self.routingMachine.getFullRoute(startingCoordinate, endingCoordinate)
        routeJson = osrmRouteData["routes"][0]
        self.routePolyline = self.routingMachine.decodePolyline(routeJson["geometry"])
        self.intersections = self.getIntersections(routeJson)

        return buildRoute()

    def getIntersections(self, routeJson):
        intersections = []
        for leg in routeJson["legs"]:
            for step in leg["steps"]:
                for intersection in step["intersection"]:
                    location = intersection["location"]
                    intersections.append(Coordinate(location[0], location[1]))

        return intersections

    def buildRoute():
        pass