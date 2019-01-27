from osrm import OSRM

class Stop:
    def __init__(self, coordinate):
        self.routingMachine = OSRM()
        self.Location = coordinate

    def setInfoFromPreviousStop(self, previousCoordinate, previousTime, speedTraveled, carModel):
        self.distanceFromPreviousStop = self.calculateDistanceFrom(previousCoordinate)
        self.timeFromPreviousStop = self.calculateTimeFrom(previousTime,speedTraveled)
        self.energyExpended = self.calculateEnergyExpended(carModel,previousCoordinate, speedTraveled)

    def calculateDistanceFrom(self, previousCoordinate):
        data = self.routingMachine.getSimpleRoute(previousCoordinate, self.Location)
        return float(data["routes"][0]["legs"][0]["distance"])

    def calculateTimeFrom(self, previousTime, speed):
        return self.distanceFromPreviousStop / speed

    def calculateEnergyExpended(self, carModel, previousCoordinate, speedTraveled):
        return carModel.drive(self.distanceFromPreviousStop, self.routingMachine.getElevationFromSegment(previousCoordinate,self.Location), speedTraveled)
