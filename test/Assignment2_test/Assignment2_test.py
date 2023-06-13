import unittest
from src.Assignment_2.utils import *


class MyTestCase(unittest.TestCase):
    def test_something(self):
        spark = session_object()
        rdd = text_to_rdd(spark)
        count_lines = 281234
        warn_ct = 3811
        api_ct = 22895
        #Testing the count of lines in rdd
        actual_input_count = textfile_count(rdd)
        expected_output = count_lines
        self.assertEqual(actual_input_count, expected_output)

        #counting number of warn logs
        actual_input_warn_count = warn_count(rdd)
        expected_warn_count = warn_ct
        self.assertEqual(actual_input_warn_count, expected_warn_count)

        #Repositories count in apl_client
        actual_input_api_client = api_count(rdd)
        expected_api_client = api_ct
        self.assertEqual(actual_input_api_client, expected_api_client)

        client_schema = StructType([
            StructField('clt_id', StringType(), True),
            StructField('count_total', IntegerType(), True)
        ])
        expected_clt_data = [("ghtorrent-12",5859)]
        expected_fail_count = (2788,)
        expected_fail_count = 130023
        #Repositories processed
        split_df = repository_processed(spark)
        actual_rep = extract_df(split_df)
        expected_rep = expected_clt_data

        #Repositories failed
        actual_input_failed_repo = failed_requests(split_df)
        expected_failed_repo = expected_fail_count
        self.assertEqual(actual_input_failed_repo, expected_failed_repo)

        #Repositories active
        actual_input_active_repo = active_repository(split_df)
        expected_active_repo = 130023
        self.assertEqual(actual_input_failed_repo, expected_failed_repo)

if __name__ == '__main__':
    unittest.main()
