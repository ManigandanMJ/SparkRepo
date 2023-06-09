from src.Assignment_1.utils import *

#Creating spark session
spark = session_object()

#reading dataframes
user_df = dataframe_read(spark)
transact_df = dataframe_read_transact(spark)

#joining dataframes
user_df_col = "user_id"
transaction_df_col = "userid"
join_type = "inner"
combine_df = dataframe_join(user_df,transact_df,user_df_col,transaction_df_col,join_type)
combine_df.show()

#count of location
location_count(combine_df)
#combine_df.show()

#product bought
product_user(combine_df)


#total spend of products
total_spend(combine_df)