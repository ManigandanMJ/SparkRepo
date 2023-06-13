import unittest
from src.Assignment_1.utils import *
from pyspark.sql.types import *


class MyTestCase(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.spark = session_object()

    def test_something(self):
        # actual dataframe schema for users
        user_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True)
        ])
        # actual data for users
        user_data = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")
                     ]
        # creating to dataframes
        user_df = self.spark.createDataFrame(data=user_data, schema=user_schema)
        # actual dataframe schema for transaction
        transaction_schema = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        ##actual dataframe data for transaction
        transaction_data = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")
                            ]
        transact_df = self.spark.createDataFrame(data=transaction_data, schema=transaction_schema)
        # Expected dataframe schema
        expected_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True),
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        # Expected data
        expected_data = [(101, "abc.123@gmail.com","hindi","mumbai",3300104,1000004,101,35000,"fridge"),
                         (101, "abc.123@gmail.com","hindi","mumbai",3300101,1000001,101,700,"mouse"),
                         (102,"jhon@gmail.com","english","usa",3300102,1000002,102,900,"keyboard"),
                         (103,"madan.44@gmail.com","marathi","nagpur",3300103,1000003,103,34000,"tv"),
                         (105,"sahil.55@gmail.com","english","usa",3300105,1000005,105,55000,"sofa")
                         ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        user_df_col = "user_id"
        transaction_df_col = "userid"
        join_type = "inner"
        transformdata_df = dataframe_join(user_df, transact_df, user_df_col, transaction_df_col, join_type)
        #transformdata_df = dataframe_join(user_df, transact_df)
        self.assertEqual(sorted(transformdata_df.collect()),sorted(expected_df.collect())) # add assertion here


        # unique location testcase
        exp_location_schema = StructType([
            StructField('product_description', StringType(), True),
            StructField('location', StringType(), True),
            StructField('count_of_location',IntegerType(),True)
        ])
        exp_location_data = [("mouse","mumbai",1),
                             ("tv","nagpur",1),
                             ("keyboard","usa",1),
                             ("sofa","usa",1),
                             ("fridge","mumbai",1)
                             ]
        exp_location_df = self.spark.createDataFrame(data=exp_location_data, schema=exp_location_schema)
        act_location_df = location_count(transformdata_df)
        self.assertEqual(sorted(act_location_df.collect()),sorted(exp_location_df.collect()))

        #Products bought by user
        exp_product_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('products bought', IntegerType(), True),
        ])
        exp_product_data = [(101,2),
                             (103,1),
                             (102,1),
                             (105,1)]
        exp_product_df = self.spark.createDataFrame(data=exp_product_data, schema=exp_product_schema)
        act_product_df = product_user(transformdata_df)
        self.assertEqual(sorted(act_product_df.collect()),sorted(exp_product_df.collect()))
        # Total_spend testcase
        exp_total_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('sum(price)',IntegerType(),True)
        ])
        exp_total_data = [(101,1000001,700),
                          (101,1000004,35000),
                          (102, 1000002, 900),
                          (103, 1000003, 34000),
                          (105,1000005,55000)
                        ]
        exp_total_df = self.spark.createDataFrame(data=exp_total_data, schema=exp_total_schema)
        act_total_df = total_spend(transformdata_df)
        self.assertEqual(sorted(act_total_df.collect()),sorted(exp_total_df.collect()))

if __name__ == '__main__':
    unittest.main()
