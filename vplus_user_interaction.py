# Import Library
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import boto3
import os

# Function range date
def daterange(start_date, end_date):
    dtx = []
    for n in range(int((end_date - start_date).days)):
        x = start_date + timedelta(n)
        dtx.append(x)
    return dtx

# Connect to database and AWS Amazon S3
try:
    ip_address = 'host'
    port = 'port'
    username = 'username' 
    password = 'password'
    dbname = 'database'
    postgres_sql = f'postgresql://{username}:{password}@{ip_address}:{port}/{dbname}'
    
    conn = create_engine(postgres_sql)
    session = boto3.Session(
    aws_access_key_id="key access ID AWS",
    aws_secret_access_key="Secret key AWS",
    )
    s3_1 = session.client('s3')
    print("Connect to database and amazon success")
except:
    print("Connect to database and amazon failed")
   
# Input variable and input date
try:
    folder_output = "path to folder output"
    main_folder = "path to main folder"
    folder_config = "path to config folder"
    file_code = "query.txt"
    with open(folder_config+file_code, 'r') as file:
        files1 = file.read().replace('\n',' ').replace('\t', '        ')
    inp_range = str(input('range date? (Y/n):'))
    print('masukkan dengan format yyyymmdd')
    if inp_range=='y' or inp_range=='Y':
        date_start,date_end = datetime.strptime((str(input('start date: '))),"%Y%m%d"), datetime.strptime((str(input('end date: '))),'%Y%m%d')
        date_end = date_end + timedelta(1)
        dates = daterange(date_start, date_end)        
    elif inp_range=='n' or inp_range=='N':
        dates = [str(input('input tanggal: '))]
    else:
        print('wrong input')
    print("Load variable success")
except:
    print("Load variable failed")
    
# Load data to S3
try:
    for dt in dates:
        if type(dt)!=str: 
            x = str(dt.strftime('%Y%m%d'))
        else:
            x = dt
        query_df = files1.format(x)
        df_interact= pd.read_sql(query_df, conn)
        df_interact.to_csv(folder_output+"path file".format(x),index=False,compression="gzip")
        s3_1.upload_file(Bucket="bucket-name", Key="path file".format(x), Filename=folder_output+"path file".format(x))
        os.remove(folder_output+"path file".format(x))
        print("tanggal {0} sukses".format(x))
    print('upload to amazon success')
except:
    print('upload to amazon failed')
    
    
    
    
    
    
