from src.Assignment_1.utils import *

#Creating spark session
spark = session_object()

#reading dataframes
user_df = dataframe_read(spark)
transact_df = dataframe_read_transact(spark)

#joining dataframes
dataframe_join(user_df,transact_df)
combine_df = dataframe_join(user_df,transact_df)

#count of location
location_count(combine_df).show()

#product bought
product_user(user_df,transact_df)

#total spend of products
total_spend(user_df,transact_df)