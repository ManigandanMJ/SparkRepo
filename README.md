**Spark Assignments**


**Question-1**


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


2.Find out products bought by each user. 


     •	 Created a new function to get the number of products bought by each user.
     •	  Using groupBy grouped userid.
     •	  Using aggregation function counting each products bought by userid.
     •	  Included the log messages.
     •	  Returned the counted values for each userid and saved in dataframe.
     •	  Called the functions and returned the dataframe.
     •	  Tested the actual input with expected output in test cases.
 

3.Total spending done by each user on each product.


      •	  Created new function for adding the all the products bought by user.
      •	  Created a dataframe using the groupBy for userid and productid.
      •	  Used aggregate function to sum the total price of each products.
      •	  Assigned values in new data frames.
      •	  Called the function in and passed the dataframe.
      •	  Tested the actual input with expected output in test cases.


**Question-2**


1.	Write a function to load csv file into rdd.



    •	Imported loggers for logging the information.
    •	Defined a method to create the session object and configured the file path.
    •	Using sparkContext read the file to rdd.
    •	And stored in another rdd and verified testcases actual and expected.
    
2.	Number of lines rdd file contains.



    •	Called the rdd file from the function created through driver.
    •	Using count function rdd lines count is found.
    •	Storing the information in the log file.
    
3.	Fing the number of times the “WARN ”  is repeated in rdd file.



    •	Filtered the line within lambda, and counted the number of warn word in file.
    •	Assigned to a variable and showed in logfile.
    •	Using test cases actual and expected is similar and verified.
    
4.	Count number of api_clients in the file.



    •	Read the rdd file and stored in another file.
    •	Using filter separated the lines containing  INFO and api_client.
    •	If condition applies, counted the words in the file.
    •	Verified the testcases by actual and expected values.
    
5.	Client did most HTTP requests in the file.



    •	Created schema and created dataframe.
    •	Assigned values from the file to dataframe with different colums.
    •	Separated each file and found client and count fields.
    •	Using groupBy filtered and counted the most requested client.
    
6.	Most failed clients.



    •	Reading rdd file and filtered using lambda function.
    •	Found mostly failed client using failed keyword.
    •	Received lines of INFO or WARN and Failed.
    •	Counted the filtered lines and verified with testcases.
    
7.	Most active repository.



    •	Counted the number of ghtorrent and exists lines.
    •	And counted the number of lines total.
    •	Received the active repositories.
    •	Verified the testcases using the actual and expected values.


