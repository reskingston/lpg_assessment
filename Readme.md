This repository is for the LPG assessment.

File description:

1. task_one.sql - The file has the code for the task no 1 - cancellation rate.
2. task_two.sql - The file has the code for the task no 2 - top 3 salaries. 
3. load_google_trends_to_redshift.py - Part of Task 3 and the functionality is explained below .
4. redshift_db_code.sql - Part of Task 3 and the functionality is explained below .                            
                                          
Task 3:

For task 3, I have used python as programming language and  Redshift database. 

The idea behind this is to 
   1. use the Jenkins to run the python code at the desired interval (running the job every hour).
   2. python code to extract data from google trends and load to raw schema in redshift.
   3. Since LPG follow ELT, We will load the data to the database before transforming it. 
   4. The sql insert statement is used to load the data from raw to bi schema. Raw schema has truncate and load tables. facts and dimensions are created in BI layer and it will be incremental load.
   5. The same or different jenkins job can be used to run the sql statement (insert).

The load_google_trends_to_redshift.py does the following tasks:
  1. extracting the google trends data for interest over time and interest by region into csv files
  2. loading the csv files to s3
  3. loading the data from s3 to redshift( raw schema ) using COPY Command.
  
The redshift_db_code.sql does the following:
  1. insert statements to load the data into bi schema. 
  
Things not done as part of this:
  1. no set up is done ( redshift, s3 buckets or jenkins jobs).
  2. Job scheduling.
  3. Code can be reused to load historical data. so, no separate code is written for the historical load.
  4. Automation testing is not done for this. 
  
Note: The way the columns are handled in the insert statement in BI schema can be changed if we had to have more than 5 key words.
  
 
  



 
