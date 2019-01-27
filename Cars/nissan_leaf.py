from car import Car

class NissanLeaf(Car):
    nissan_leaf_battery_capacity = 40
    nissan_leaf_system_voltage = 360
    def __init__(self):
        super().__init__(self.nissan_leaf_battery_capacity, self.nissan_leaf_system_voltage)