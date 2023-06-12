from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
import logging
from pyspark.sql.types import *

logging.basicConfig(filename="c:\\logs\\spark2.log", filemode="w")
log = logging.getLogger()
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

# Repositories processed
def client_requests(rdd_data):
    client_lines = rdd_data.filter(lambda line: "INFO" in line and "api_client" in line and "URL" in line)
    delimiter1 = ','
    delimiter2 = '--'
    requests_client = client_lines.flatMap(lambda line: line.split(delimiter1)) \
        .flatMap(lambda line: line.split(delimiter2)) \
        .reduceByKey(lambda a, b: a + b)\
        # .max(key=lambda x: x[1])
    log.warning("%s Number of lines in RDD" % requests_client)

    return requests_client





# Repositories failed from rrd
def failed_requests(rdd_data):
    fail_req = rdd_data.filter(lambda line: "INFO" in line or "WARN" in line and "Failed" in line).count()
    log.warning("%s Number of lines in RDD" % fail_req)
    return fail_req


# Repository active mostly
def active_repository(rdd_data):
    repository_active = rdd_data.filter(lambda x: 'ghtorrent.rb' in x and 'exists' in x).count()
    log.warning("%s Number of lines in RDD" % repository_active)
    return repository_active
