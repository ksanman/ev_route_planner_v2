from osrm import OSRM
import unittest

class Test_NissanLeaf(unittest.TestCase):
    def initialize(self):
        return OSRM()

    def test_initialize(self):
        pass

    def test_decodePolyline(self):
        polyline = "}wv}FfpqiTCnE`KHpEJrCBX?T?vA@nA?nA@vA@T@X?T@dCBx@?P@X?D?hA@r@@v@?`A@zDBfFDpB@P?|JJtJJdEBvDB`EFlFHzA\\x@Jt@FdATbAX`Ab@f@V~Az@`Bz@^DjA|@n@j@bAdApAzA`AvA~BhD^h@vCdEjFxHjAdB^j@vEfHtFdIrHpLdBnCXd@z@jA`D~ElC~DlDpFrHfLpDjFvBbDpAnBzA|BrAzBd@r@dBtCxBhDzAnBbCrDV`@~B`D|@zAfDhE~CxDr@t@jIbI~Z`[lFnFnFpFrIhIxH|HhGfGhAjAtAtAhCjCrEnE~IxIt[v[lTbTbTpTrMjNbQjRtYbYxUzV|HrI"
        rm = self.initialize()
        decoded = rm.decodePolyline(polyline)

        textCoords = []
        with open("coordinates.txt", "r") as file:
            coords = file.readlines()
        
        for coord in coords:
            if coord == '\n':
                continue
            numbers = coord.split(" ")
            textCoords.append((float(numbers[0]), float(numbers[1].replace("\n",""))))
            
        for decode, test in zip(decoded, textCoords):
            self.assertEqual(decode, test)


if __name__ == '__main__':
    unittest.main(exit=False)