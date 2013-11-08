import unittest
from swift.common.swob import Request
from swift.common.request_helpers import fingerprint2path_and_validate
from swift.common.swob import HTTPBadRequest

class simpleTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_finger_to_path(self):
        request = Request.blank('/sda1/p/fingerprint/uni-refer')
        self.assertEqual(fingerprint2path_and_validate(request, 4), ['sda1', 'p', 'fingerprint', 'uni-refer'])
        self.assertRaises(Exception, fingerprint2path_and_validate, request, 5)
        self.assertEqual(fingerprint2path_and_validate(request, 3), ['sda1', 'p', 'fingerprint/uni-refer'])

if '__main__' == __name__:
    unittest.main()
