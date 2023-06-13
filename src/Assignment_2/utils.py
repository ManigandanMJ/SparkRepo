from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
import logging
from pyspark.sql.types import *

logging.basicConfig(filename="c:\\logs\\spark2.log", filemode="w")
log = logging.getLogger('mylogger')
log.setLevel(logging.DEBUG)


# session_object function
def session_object():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    log.info("created spark session")
    return spark


# Reading the text file and creating rdd
def text_to_rdd(spark):
    rdd = spark.sparkContext.textFile("../../resource/ghtorrent-logs.txt")
    log.info("created rdd file from textfile")
    return rdd


# The count of lines in rdd
def textfile_count(rdd):
    line_count = rdd.count()
    log.warning("%s Number of lines in RDD" % line_count)
    log.error("%s Number of lines in RDD" % line_count)
    return line_count


# counting number of warn logs
def warn_count(rdd_data):
    word_count = rdd_data.filter(lambda line: "WARN" in line).count()
    log.warning("%s Number of lines in RDD" % word_count)
    return word_count


# counting number of apli_clients
def api_count(rdd_data):
    line_count1 = rdd_data.filter(lambda line: "INFO" in line and "api_client" in line).count()
    log.warning("%s Number of lines in RDD" % line_count1)
    return line_count1


def repository_processed(spark):
    torrent_schema = StructType([
        StructField('log_msg', StringType(), True),
        StructField('date_time', StringType(), True),
        StructField('client_id', StringType(), True)
    ])
    df = spark.read.schema(torrent_schema).csv("../../resource/ghtorrent-logs.txt")

    # df.show()
    return df


def extract_df(df):
    df_col = df.withColumn('clt_id', split(col('client_id'), '--').getItem(0)) \
        .withColumn('api_client', split(col('client_id'), '--').getItem(1)) \
        .withColumn('api_clt', split(col('api_client'), ':').getItem(0)) \
        .withColumn('url', split(col('api_client'), ':').getItem(1)) \
        .withColumn('status', split(col('url'), ' ').getItem(0)) \
        .withColumn('aft_status', split(col('url'), ' ').getItem(1))
    df_col.show()
    return df_col


def max_count(df_col):
    counts_df = df_col.groupBy("clt_id").agg(count("clt_id").alias("count_total")).orderBy(col("count_total").desc())
    max_val = counts_df.show(1)
    return max_val


# Repositories failed from rrd
def failed_requests(df_col):
    filtered_df = df_col.filter(col("aft_status").like("%Failed%")).agg(count("*").alias("count_max"))
    log.warning("%s Number of lines in RDD" % filtered_df)
    return filtered_df.show()


# Repository active mostly
def active_repository(rdd_data):
    repository_active = rdd_data.filter(lambda x: 'ghtorrent.rb' in x and 'exists' in x).count()
    log.warning("%s Number of lines in RDD" % repository_active)
    return repository_active
