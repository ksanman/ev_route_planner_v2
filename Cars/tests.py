from battery import Battery
from nissan_leaf_battery import NissanLeafBattery
import unittest

class Test_Car(unittest.TestCase):
    def initialize(self):
        return Battery(100, 360)

    def test_initialize(self):
        car = self.initialize()
        self.assertEqual(car.kwh_to_watt_hour_conversion_factor, 1000)
        self.assertEqual(car.lion_battery_efficiency, .90)
        self.assertEqual(car.batteryCapacityKwh, 100)
        self.assertEqual(car.systemVoltage, 360)

    def test_kwhToAh(self):
        car = self.initialize()
        self.assertEqual(car.kwhToAh(100), 277.77777777777777)
        self.assertEqual(car.batteryCapacityAh, 277.77777777777777)

    def test_ahToKwh(self):
        car = self.initialize()
        self.assertEqual(car.ahToKwh(277.77777777777777), 100)

    def test_charge(self):
        car = self.initialize()
        self.assertEqual(car.charge(.75, 10), 0.024300000000000002)

class Test_NissanLeaf(unittest.TestCase):
    def initialize(self):
        return NissanLeafBattery()

    def test_initialize(self):
        car = self.initialize()
        self.assertEqual(car.nissan_leaf_battery_capacity, 40)
        self.assertEqual(car.nissan_leaf_system_voltage, 360)
        self.assertEqual(car.kwh_to_watt_hour_conversion_factor, 1000)
        self.assertEqual(car.lion_battery_efficiency, .90)
        self.assertEqual(car.batteryCapacityKwh, 40)
        self.assertEqual(car.systemVoltage, 360)

    def test_charge(self):
        car = self.initialize()
        self.assertEqual(car.charge(.75, 10), .060750000000000005)

if __name__ == '__main__':
    unittest.main(exit=False)