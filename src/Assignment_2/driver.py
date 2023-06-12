from src.Assignment_2.utils import *

spark = session_object()
rdd = text_to_rdd(spark)

# The count of lines in rdd
textfile_count(rdd)

# counting number of warn logs
warn_count(rdd)

#count of in apl_client
api_count(rdd)

#Repositories processed
client_requests(rdd)

#Repositories failed
failed_requests(rdd)

#Repositories active
active_repository(rdd)

