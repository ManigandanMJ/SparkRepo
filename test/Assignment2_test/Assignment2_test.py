import unittest
from src.Assignment_2.utils import *


class MyTestCase(unittest.TestCase):
    def test_something(self):
        spark = session_object()
        rdd = text_to_rdd(spark)

        #Testing the count of lines in rdd
        actual_input_count = textfile_count(rdd)
        expected_output = 281234
        self.assertEqual(actual_input_count, expected_output)

        #counting number of warn logs
        actual_input_warn_count = warn_count(rdd)
        expected_warn_count = 3811
        self.assertEqual(actual_input_warn_count, expected_warn_count)

        #Repositories porcessed in apl_client
        actual_input_api_client = client_requests(rdd)
        expected_api_client = 3811
        self.assertEqual(actual_input_api_client, expected_api_client)

        #Repositories failed
        actual_input_failed_repo = failed_requests(rdd)
        expected_failed_repo = 65397
        self.assertEqual(actual_input_failed_repo, expected_failed_repo)

        #Repositories active
        actual_input_active_repo = active_repository(rdd)
        expected_active_repo = 130023
        self.assertEqual(actual_input_failed_repo, expected_failed_repo)

if __name__ == '__main__':
    unittest.main()
