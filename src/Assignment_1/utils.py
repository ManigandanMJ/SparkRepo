from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#session_object function
def session_object():
    #creating spark session
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def dataframe_read(spark):
#creating dataframe for user file
    user_df = spark.read.option("header",True).options(inferSchema='True',delimiter=',') \
              .csv("../../resource/user.csv")


    return user_df
def dataframe_read_transact(spark):
    #creating dataframe for transact file
    transact_df = session_object().read.option("header",True).options(inferSchema='True',delimiter=',')\
                      .csv("../../resource/transaction.csv")
    return transact_df
#Joining dataframes function
def dataframe_join(user_df,transact_df):
    #joining the data frames with joins
    combine_df = user_df.join(transact_df,user_df.user_id == transact_df.userid,"inner")
    #combine_df.show()
    return combine_df

# location count function
def location_count(combine_df):
    #groupBy and counting the unique location
    count_df = combine_df.groupBy("product_description","location").agg(count("location").alias("count_of_location"))
    #Display count of loacation
    return count_df

def product_user(user_df,transact_df):

    product_df = dataframe_join(user_df,transact_df).groupBy("userid").agg(collect_set("product_description")).alias("products bought")
    return product_df.show()

def total_spend(user_df,transact_df):
        total_df = dataframe_join(user_df,transact_df).groupBy("userid","product_id").agg(sum("price"))
        return total_df.show()

