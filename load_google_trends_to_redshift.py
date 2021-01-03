#!/usr/bin/env python3

from pytrends.request import TrendReq
import boto3
import psycopg2

pytrends = TrendReq()
kw_list = ["Elon Musk","Joe Biden","Taylor Swift","Godzilla","Black Panther"]

 #the first load will use today 5-y to load all the data for the last 5 years and now 1-H will load last 1 hour data
pytrends.build_payload(kw_list, cat=0, timeframe='now 1-H', geo='', gprop='')

#extract the data as csv files
data = pytrends.interest_over_time()
data.to_csv('interest_over_time_file.csv', encoding='utf_8_sig')

citydata = pytrends.interest_by_region()
citydata.to_csv('interest_by_region_file.csv', encoding='utf_8_sig')



#upload the files to S3
s3 = boto3.resource('s3')
#mention the correct bucket name and the folder path. date can be added to the file name or we can store the files under each day (if needed)
s3.meta.client.upload_file('interest_over_time_file.csv', 'lpg_googletrendsdata/prod/', 'interest_over_time_file.csv') 
s3.meta.client.upload_file('interest_by_region_file.csv', 'lpg_googletrendsdata/prod/', 'interest_over_time_file.csv') 


#use copy command to load the files to redshift
conn_str = psycopg2.connect(dbname= '', host='', port= '5439', user= '', password= '')
sql_command_gtrend_iot = "copy dw_raw.gtrends_interest_over_time from 's3://lpg_googletrendsdata/prod/interest_over_time_file.csv' credentials 'aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>' delimiter ',' removequotes;"
sql_command_gtrend_ibr = "copy dw_raw.gtrends_interest_by_region from 's3://lpg_googletrendsdata/prod/interest_by_region_file.csv' credentials 'aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>' delimiter ',' removequotes;"

cur = conn_str.cursor()


cur.execute('truncate table dw_raw.gtrends_interest_over_time;')
cur.execute(sql_command_gtrend_iot)
cur.execute('truncate table dw_raw.gtrends_interest_by_region;')
cur.execute(sql_command_gtrend_ibr)

conn_str.commit()

cur.close()    
conn_str.close()

