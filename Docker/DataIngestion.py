
# coding: utf-8

# In[3]:

import json
import requests
import csv
import pandas as pd
import os
import boto3
from boto3.s3.transfer import S3Transfer
import sys


# In[ ]:

def timeSeriesDataIngestion():
    
    homepath = os.path.expanduser('~')
    
    country_list = ['BRA', 'IND', 'ZAF', 'ECU', 'ARG', 'LBY']

    indicator_list = ['AG.LND.AGRI.ZS', 'SP.POP.DPND', 'SP.DYN.CBRT.IN', 'NE.EXP.GNFS.ZS', 'NY.GDP.MKTP.CD',                       'NY.GDP.MKTP.KD.ZG', 'SP.POP.GROW', 'FI.RES.TOTL.CD', 'NE.TRD.GNFS.ZS']
    
    indicator_value = []

    for country in country_list:
        for indicator in indicator_list:
            indicator_response = requests.get('http://api.worldbank.org/countries/'                                               + country + '/indicators/' + indicator + '?format=json&per_page=1000')

            filepath = './Data/TimeSeries/' + indicator + '/'
            filename = filepath + country + '.json'
        
            if not os.path.exists(filepath):            
                print('Creating required directories for Timeseries!!', '\n')
                os.makedirs(filepath)
            
            with open(filename, 'w') as f:
                f.write(indicator_response.text)

            indicator_response_toFile = open(filename, 'r', errors = 'ignore') # Opening the file
            indicator_data = json.load(indicator_response_toFile)            

            if indicator_data[0]['total'] > 0:
                for data in indicator_data[1:]:
                    for yeardata in data:                
                        indicator_value.append([yeardata['country']['value'], yeardata['country']['id'],                                                 yeardata['indicator']['id'], yeardata['value'],                                                 yeardata['indicator']['value'], yeardata['date'] + '-01-01'])
    
    headers = ['CountryName', 'CountryCode', 'IndicatorCode', 'Value', 'Value Description', 'Year']
    indicator_df = pd.DataFrame(indicator_value, columns=headers)
    
    actual_filename = './Data/TimeSeries/Indicators_TimeSeries_Combined.csv'
    indicator_df.to_csv(actual_filename, index=False)
    print('Timeseries file created!!', '\n')

    #indicator_df.head()


# In[5]:

def clusteringDataIngestion():
    
    homepath = os.path.expanduser('~')
    
    country_list = ['AUS', 'CAN', 'SAU', 'USA',                     'IND', 'RUS', 'ZAF', 'TUR',                     'ARG', 'BRA', 'MEX',                     'FRA', 'DEU', 'ITA', 'GBR',                     'CHN', 'IDN', 'JPN']

    indicator_list = ['AG.LND.AGRI.ZS', 'SP.POP.DPND', 'SP.DYN.CBRT.IN', 'NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS',                       'NY.GDP.MKTP.CD', 'NY.GDP.MKTP.KD.ZG', 'SP.POP.GROW', 'FI.RES.TOTL.CD', 'NE.TRD.GNFS.ZS']
    
    indicator_value = []

    for country in country_list:
        for indicator in indicator_list:
            indicator_response = requests.get('http://api.worldbank.org/countries/'                                               + country + '/indicators/' + indicator +                                               '?format=json&per_page=1000&date=2001:2016')

            filepath = './Data/Clustering/' + indicator + '/'
            filename = filepath + country + '.json'
        
            if not os.path.exists(filepath):            
                print('Creating required directories for Clustering!!', '\n')
                os.makedirs(filepath)
            
            with open(filename, 'w') as f:
                f.write(indicator_response.text)

            indicator_response_toFile = open(filename, 'r', errors = 'ignore') # Opening the file
            indicator_data = json.load(indicator_response_toFile)            

            if indicator_data[0]['total'] > 0:
                for data in indicator_data[1:]:
                    for yeardata in data:                
                        indicator_value.append([yeardata['country']['value'], yeardata['country']['id'],                                                 yeardata['indicator']['id'], yeardata['value'],                                                 yeardata['indicator']['value'], yeardata['date'] + '-01-01'])
    
    headers = ['CountryName', 'CountryCode', 'IndicatorCode', 'Value', 'Value Description', 'Year']
    indicator_df = pd.DataFrame(indicator_value, columns=headers)
    
    actual_filename = './Data/Clustering/Indicators_Clustering_Combined.csv'
    indicator_df.to_csv(actual_filename, index=False)
    print('Clustering file created!!', '\n')


# In[33]:

def timeseriesFileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    
    conn = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    transfer = S3Transfer(conn)

    response = conn.list_buckets()    
    existent = []
    for bucket in response["Buckets"]:
        existent.append(bucket['Name'])

    bucket_name = 'Team6FinalProject'
    target_dir = './Data/TimeSeries/'
    filenames = []
    file_list = os.listdir(target_dir)
    for file in file_list:
        if '_Combined' in file:
            filenames.append(file)

    if bucket_name in existent:
        print('Bucket already exists!!', '\n')
        print('Timeseries Files upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'TimeSeries/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename)
        print('Timeseries Files uploaded to s3!!!!!','\n')
            
    else:
        print('Bucket not present. Created bucket!!', '\n')
        conn.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        print('Timeseries Files upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'TimeSeries/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename)
        print('Timeseries Files uploaded to s3!!!!!','\n')


# In[1]:

def clusteringFileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    
    conn = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    transfer = S3Transfer(conn)

    response = conn.list_buckets()    
    existent = []
    for bucket in response["Buckets"]:
        existent.append(bucket['Name'])

    bucket_name = 'Team6FinalProject'
    homepath = os.path.expanduser('~')
    target_dir = './Data/Clustering/'
    filenames = []
    file_list = os.listdir(target_dir)
    for file in file_list:
        if '_Combined' in file:
            filenames.append(file)

    if bucket_name in existent:
        print('Bucket already exists!!', '\n')
        print('CLustering Files upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'Clustering/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('Clustering Files uploaded to s3!!!!!','\n')
            
    else:
        print('Bucket not present. Created bucket!!', '\n')
        conn.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        print('CLustering Files upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'Clustering/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('CLustering Files uploaded to s3!!!!!','\n')
    


# In[26]:

def main():
    
    user_input = sys.argv[1:]
    print("----Process Started----")
    counter = 0
    if len(user_input) == 0:
        print('No Input provided. Process is exiting!!')
        exit(0)
    for ip in user_input:
        if counter == 0:
            AWS_ACCESS_KEY = str(ip)
        else:
            AWS_SECRET_KEY = str(ip)
        counter += 1
    
    timeSeriesDataIngestion()
    clusteringDataIngestion()
    timeseriesFileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    clusteringFileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    
    print('Process completed!!','\n')


# In[ ]:

if __name__ == '__main__':
    main()

