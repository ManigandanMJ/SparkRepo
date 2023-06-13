from src.Assignment_2.utils import *

spark = session_object()
rdd = text_to_rdd(spark)

# The count of lines in rdd
textfile_count(rdd)
# counting number of warn logs
warn_count(rdd)
#count of apl_client
api_count(rdd)
#using extracted dataframe
df = repository_processed(spark)
df_col = extract_df(df)
#Maximum processed client
max_count(df_col)
#Repositories failed
failed_requests(df_col)
#Repositories active
active_repository(rdd)

