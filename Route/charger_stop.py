from stop import Stop

class ChargerStop(Stop):
    def __init__(self, coordinate, charger):
        super().__init__(coordinate)
        self.charger = charger