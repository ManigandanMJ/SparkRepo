#Spark Assignments#


##Question-1##


1.Count of unique locations where each product is sold. 


    •	Imported spark session from pyspark.sql.
    •	Created logger and configured.
    •	Defined a function for creating the sparksession.
    •	Created spark session and added log file.
    •	Defined a method for reading the csv file from user.csv and assigned to a new dataframe returned.
    •	Defined a method for reading the csv file from transaction.csv to a new dataframe returned.
    •	Created a new dataframe from combine 2 datafreames in new function.
    •	Created a new method for counting the unique location for each product sold.
    •	Called the functions in driver file.
    •	Tested the actual input with expected output in test cases.


2.	Find out products bought by each user. 

     •	 Created a new function to get the number of products bought by each user.
     •	  Using groupBy grouped userid.
     •	  Using aggregation function counting each products bought by userid.
     •	  Included the log messages.
     •	  Returned the counted values for each userid and saved in dataframe.
     •	  Called the functions and returned the dataframe.
     •	  Tested the actual input with expected output in test cases.
 
3.	Total spending done by each user on each product.

      •	  Created new function for adding the all the products bought by user.
      •	  Created a dataframe using the groupBy for userid and productid.
      •	  Used aggregate function to sum the total price of each products.
      •	  Assigned values in new data frames.
      •	  Called the function in and passed the dataframe.
      •	  Tested the actual input with expected output in test cases.
