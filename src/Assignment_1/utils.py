from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(filename="c:\\logs\\spark1.log", filemode="w")
log = logging.getLogger()
log.setLevel(logging.INFO)

# session_object function
def session_object():
    # creating spark session
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    log.info("Session for spark created")
    return spark


def dataframe_read(spark):
    # creating dataframe for user file
    user_df = spark.read.option("header", True).options(inferSchema='True', delimiter=',') \
        .csv("../../resource/user.csv")
    log.info("Reading dataframe for user.csv")
    return user_df


def dataframe_read_transact(spark):
    # creating dataframe for transact file
    transact_df = session_object().read.option("header", True).options(inferSchema='True', delimiter=',') \
        .csv("../../resource/transaction.csv")
    log.info("Reading dataframe for transaction.csv")
    return transact_df


# Joining dataframes function
def dataframe_join(user_df, transact_df,user_df_col,transaction_df_col,join_type):
    # joining the data frames with joins
    combine_df = user_df.join(transact_df,user_df[user_df_col] == transact_df[transaction_df_col], join_type)
    log.info("combined dataframe for user and transaction")
    # combine_df.show()
    return combine_df


# location count function
def location_count(combine_df):
    # groupBy and counting the unique location
    count_df = combine_df.groupBy("product_description", "location").agg(count("location").alias("count_of_location"))
    log.info("Joining dataframe for user and transaction")
    # Display count of loacation
    return count_df


def product_user(combine_df):
    product_df = combine_df.groupBy("userid").agg(count("product_description").alias("products bought"))
    log.info("Product_user method executed")
    return product_df


def total_spend(combine_df):
    total_df = combine_df.groupBy("userid", "product_id").agg(sum("price"))
    log.info("total_spend method executed")
    return total_df
