from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#session_object function
def session_object():
    #creating spark session
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

#Joining dataframes function
def dataframe_join():
    #creating dataframe for user file
    user_df = session_object().read.option("header",True).options(inferSchema='True',delimiter=',') \
          .csv("../../resource/user.csv")

    #creating dataframe for transact file
    transact_df = session_object().read.option("header",True).options(inferSchema='True',delimiter=',')\
                  .csv("../../resource/transaction.csv")

    #joining the data frames with joins
    combine_df = user_df.join(transact_df,user_df.user_id == transact_df.userid,"inner")
    return combine_df

# location count function
def location_count():
    #groupBy and counting the unique location
    count_df = dataframe_join().groupBy("product_description","location").agg(count("location").alias("count_of_location"))
    #Display count of loacation
    return count_df.show()

