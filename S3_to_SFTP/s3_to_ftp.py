import pysftp 
import paramiko
import boto3
import json 
import os
import csv
from datetime import date, datetime

FTP_path = 'Import/SFMC_MAHUS_PROD_FTP_IMPORT'
FTP_user = '514004769_6'
FTP_pwd = 'wXLgwjwumwcB1!'
FTP_host = 'mcfkg9pykfxy9m3q8yxkthl6zwbq.ftp.marketingcloudops.com'

s3 = boto3.resource('s3')


def check(data):
    data = data.split('/')[-2]
    dat = datetime.now()
    dat = dat.strftime('%Y%m%d')
    
    if data == 'OPT_IN':
        data = data+'_'+dat
        return data
    elif data == 'PET_OWNER':
        data = data+'_'+dat
        return data
    elif data == 'PET':
        data = data+'_'+dat
        return data
    elif data == 'PRODUCT_ORDER':
        data = data+'_'+dat
        return data
    elif data == 'SUBSCRIPTION':
        data = data+'_'+dat
        return data

def lambda_handler(event,context):
    if event:
        for i in event['Records']:
            bucket = i['s3']['bucket']['name']
            key = i['s3']['object']['key']
            
           
            k = check(key)
            local_file = '/tmp/'+'MAHUS_DE_PROD_'+ k +'.csv'
            s3.Bucket(bucket).download_file(key,local_file)
    # os.chdir('/tmp')
    # with open(local_file,'r') as f:
    #     reader = csv.DictReader(f)
    #     for i in reader:
    #         print(i)
            
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    connection = pysftp.Connection(host= FTP_host, username= FTP_user, password= FTP_pwd,cnopts= cnopts)

    with connection.cd(FTP_path):
        connection.put(local_file)
    print('File Uploaded Successfully,'+str(local_file))