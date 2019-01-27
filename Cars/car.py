class Car:
    kwh_to_watt_hour_conversion_factor = 1000
    lion_battery_efficiency = .90

    def __init__(self, batteryCapacityKwh, systemVoltage):
        """
        Creates a new instance of the Car object. 
        """
        self.batteryCapacityKwh = batteryCapacityKwh
        self.systemVoltage = systemVoltage
        self.batteryCapacityAh = self.kwhToAh(self.batteryCapacityKwh)

    def kwhToAh(self, kwh):
        """
        Converts KWH to AH based on the system voltage. 
        """
        return ((kwh * self.kwh_to_watt_hour_conversion_factor) / self.systemVoltage)

    def ahToKwh(self, ah):
        """
        Converts AH to KWH based on the system voltage.
        """
        return ((ah * self.systemVoltage) / self.kwh_to_watt_hour_conversion_factor)

    def charge(self, hours, current):
        """
        Computes the charge for the specified amount of time and adds it to the current 
        battery percentage variable. 
        """
        charge = (hours * self.lion_battery_efficiency * current)
        return self.ahToKwh(charge) /  self.batteryCapacityKwh


    def drive(self, distance, elevation, speedTraveled):
        pass