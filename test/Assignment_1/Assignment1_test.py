import unittest
from src.Assignment_1.utils import *
from pyspark.sql.types import *

class MyTestCase(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.spark =session_object()


    def test_something(self):

        #actual dataframe schema for users
        user_schema = StructType([
            StructField('user_id',IntegerType(),True),
            StructField('emailid',StringType(),True),
            StructField('nativelanguage',StringType(),True),
            StructField('location',StringType(),True)
        ])
        #actual data for users
        user_data = [(101,"abc.123@gmail.com","hindi","mumbai"),
                     (102,"jhon@gmail.com","english","usa"),
                     (103,"madan.44@gmail.com","marathi","nagpur"),
                     (104,"local.88@outlook.com","tamil","chennai"),
                     (105,"sahil.55@gmail.com","english","usa"),
                     (106,"adi@gmail.com","hindi","nagpur"),
                     (107,"jason@gmail.com","marathi","mumbai"),
                     (108,"sohan@gmail.com","kannad","usa"),
                     (109,"case@outlook.com","tamil","mumbai"),
                     (110,"fury@gmail.com","hindi","nagpur"),
                    ]
        #creating to dataframes
        user_df = self.spark.createDataFrame(data=user_data, schema=user_schema)
        #actual dataframe schema for transaction
        transaction_schema = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        ##actual dataframe data for transaction
        transaction_data = [(3300101,1000001,101,700,"mouse"),
                            (3300102,1000002,102,900,"keyboard"),
                            (3300103,1000003,103,34000,"tv"),
                            (3300104,1000004,101,35000,"fridge"),
                            (3300105,1000005,105,55000,"sofa"),
                            (3300106,1000006,106,100,"bed"),
                            (3300107,1000007,105,66000,"laptop"),
                            (3300108,1000008,108,20000,"phone"),
                            (3300109,1000009,101,500,"speaker"),
                            (3300110,1000010,102,1000,"chair")
                            ]
        transact_df = self.spark.createDataFrame(data=transaction_data, schema=transaction_schema)
        #Expected dataframe schema
        Expected_schema = StructType([
            StructField('product_description', StringType(), True),
            StructField('location', StringType(), True),
            StructField('count_of_location', IntegerType(), True)
        ])
        #Expected data
        Expected_data = [("mouse","mumbai",1),
                         ("phone","usa",1),
                         ("tv", "nagpur", 1),
                         ("bed", "nagpur", 1),
                         ("laptop","usa",1),
                         ("chair","usa",1),
                         ("speaker","mumbai",1),
                         ("keyboard","usa",1),
                         ("sofa","usa",1),
                         ("fridge","mumbai",1)
                         ]

        Expected_df = self.spark.createDataFrame(data=Expected_data, schema=Expected_schema)

        transformdata_df = dataframe_join(user_df, transact_df)

        self.assertEqual(transformdata_df,Expected_df)  # add assertion here


if __name__ == '__main__':
    unittest.main()
