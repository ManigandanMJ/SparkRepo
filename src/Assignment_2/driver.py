from src.Assignment_2.utils import *

spark = session_object()
rdd = text_to_rdd(spark)

# The count of lines in rdd
textfile_count(rdd)

# counting number of warn logs
warn_count(rdd)

#count of in apl_client
api_count(rdd)
#
# #Repositories processed
# #client_requests(rdd)
# text_df(spark)
df = repository_processed(spark)

#Repositories failed
failed_requests(rdd)
df_col = extract_df(df)
max_count(df_col)

#Repositories active
active_repository(rdd)

