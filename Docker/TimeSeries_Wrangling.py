
# coding: utf-8

# In[10]:

import json
import requests
import csv
import pandas as pd
import os
import matplotlib.pylab as plt
import boto3
from boto3.s3.transfer import S3Transfer
import sys


# In[20]:

def readFile():
    
    homepath = os.path.expanduser('~')
    indicator_data = pd.read_csv('./Data/TimeSeries/Indicators_TimeSeries_Combined.csv',                                  low_memory=False)
    return indicator_data


# # Handling Missing values for Argentina

# In[12]:

def argentina():
    
    indicator_data = readFile()

    argentina_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                       (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                       (indicator_data['CountryCode'] == 'AR')]


    argentina_df_ind1['Value'] = argentina_df_ind1['Value'].interpolate()
    argentina_df_ind1['Value'] = argentina_df_ind1['Value'].fillna(method='bfill', axis = 0)

    argentina_df_ind2['Value'] = argentina_df_ind2['Value'].fillna(method='bfill', axis = 0)

    argentina_df_ind5['Value'] = argentina_df_ind5['Value'].interpolate()

    argentina_df_ind6['Value'] = argentina_df_ind6['Value'].interpolate()

    # Combining all the Argentina Dataframes

    Argentina_df = pd.concat([argentina_df_ind1, argentina_df_ind2, argentina_df_ind3, argentina_df_ind4, argentina_df_ind5,                          argentina_df_ind6, argentina_df_ind7, argentina_df_ind8, argentina_df_ind9])
    print('Timeseries Wrangling completed for Argentina!!', '\n')
    return Argentina_df


# # Handling Missing values for Brazil

# In[13]:

def brazil():
    
    indicator_data = readFile()

    brazil_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'BR')]

    brazil_df_ind1['Value'] = brazil_df_ind1['Value'].interpolate()
    brazil_df_ind1['Value'] = brazil_df_ind1['Value'].fillna(method='bfill', axis = 0)

    brazil_df_ind2['Value'] = brazil_df_ind2['Value'].fillna(method='bfill', axis = 0)

    brazil_df_ind6['Value'] = brazil_df_ind6['Value'].interpolate()

    # Combining all the Brazil Dataframes

    Brazil_df = pd.concat([brazil_df_ind1, brazil_df_ind2, brazil_df_ind3, brazil_df_ind4, brazil_df_ind5,                       brazil_df_ind6, brazil_df_ind7, brazil_df_ind8, brazil_df_ind9])
    print('Timeseries Wrangling completed for Brazil!!', '\n')
    return Brazil_df


# # Handling Missing values for Ecuador

# In[14]:

def ecuador():
    
    indicator_data = readFile()

    Ecuador_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                  (indicator_data['CountryCode'] == 'EC')]
    Ecuador_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'EC')]

    Ecuador_df_ind1['Value'] = Ecuador_df_ind1['Value'].interpolate()
    Ecuador_df_ind1['Value'] = Ecuador_df_ind1['Value'].fillna(method='bfill', axis = 0)

    Ecuador_df_ind2['Value'] = Ecuador_df_ind2['Value'].fillna(method='bfill', axis = 0)

    Ecuador_df_ind6['Value'] = Ecuador_df_ind6['Value'].interpolate()

    # Combining all the Ecuador Dataframes

    Ecuador_df = pd.concat([Ecuador_df_ind1, Ecuador_df_ind2, Ecuador_df_ind3, Ecuador_df_ind4, Ecuador_df_ind5,                          Ecuador_df_ind6, Ecuador_df_ind7, Ecuador_df_ind8, Ecuador_df_ind9])
    
    print('Timeseries Wrangling completed for Ecuador!!', '\n')
    return Ecuador_df


# # Handling Missing values for India

# In[15]:

def india():
    
    indicator_data = readFile()

    India_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'IN')]
    India_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]

    India_df_ind1['Value'] = India_df_ind1['Value'].interpolate()
    India_df_ind1['Value'] = India_df_ind1['Value'].fillna(method='bfill', axis = 0)

    India_df_ind2['Value'] = India_df_ind2['Value'].fillna(method='bfill', axis = 0)

    India_df_ind6['Value'] = India_df_ind6['Value'].interpolate()

    # Combining all the India Dataframes

    India_df = pd.concat([India_df_ind1, India_df_ind2, India_df_ind3, India_df_ind4, India_df_ind5,                      India_df_ind6, India_df_ind7, India_df_ind8, India_df_ind9])
    
    print('Timeseries Wrangling completed for India!!', '\n')
    
    return India_df


# # Handling Missing values for Libya

# In[16]:

def libya():
    
    indicator_data = readFile()

    Libya_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'LY')]
    Libya_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'LY')]

    Libya_df_ind1['Value'] = Libya_df_ind1['Value'].interpolate()
    Libya_df_ind1['Value'] = Libya_df_ind1['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind2['Value'] = Libya_df_ind2['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind4['Value'] = Libya_df_ind4['Value'].interpolate()
    Libya_df_ind4['Value'] = Libya_df_ind4['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind5['Value'] = Libya_df_ind5['Value'].interpolate()
    Libya_df_ind5['Value'] = Libya_df_ind5['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind6['Value'] = Libya_df_ind6['Value'].interpolate()
    Libya_df_ind6['Value'] = Libya_df_ind6['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind8['Value'] = Libya_df_ind8['Value'].fillna(method='bfill', axis = 0)

    Libya_df_ind9['Value'] = Libya_df_ind9['Value'].interpolate()
    Libya_df_ind9['Value'] = Libya_df_ind9['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Libya Dataframes

    Libya_df = pd.concat([Libya_df_ind1, Libya_df_ind2, Libya_df_ind3, Libya_df_ind4, Libya_df_ind5,                      Libya_df_ind6, Libya_df_ind7, Libya_df_ind8, Libya_df_ind9])
    
    print('Timeseries Wrangling completed for Libya!!', '\n')
    
    return Libya_df


# # Handling Missing values for South Africa

# In[17]:

def south_Africa():
    
    indicator_data = readFile()

    South_Africa_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    South_Africa_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]

    South_Africa_df_ind1['Value'] = South_Africa_df_ind1['Value'].interpolate()
    South_Africa_df_ind1['Value'] = South_Africa_df_ind1['Value'].fillna(method='bfill', axis = 0)

    South_Africa_df_ind2['Value'] = South_Africa_df_ind2['Value'].fillna(method='bfill', axis = 0)

    South_Africa_df_ind6['Value'] = South_Africa_df_ind6['Value'].interpolate()

    # Combining all the South_Africa Dataframes

    South_Africa_df = pd.concat([South_Africa_df_ind1, South_Africa_df_ind2, South_Africa_df_ind3, South_Africa_df_ind4,                             South_Africa_df_ind5, South_Africa_df_ind6, South_Africa_df_ind7, South_Africa_df_ind8,                              South_Africa_df_ind9])
    
    print('Timeseries Wrangling completed for South Africa!!', '\n')
    
    return South_Africa_df


# In[18]:

def writeFile():

    Argentina_df = argentina()
    Brazil_df = brazil()
    Ecuador_df = ecuador()
    India_df = india()
    Libya_df = libya()
    South_Africa_df = south_Africa()
    
    # Combining all countries DataFrame
    final_df = pd.concat([Argentina_df, Brazil_df, Ecuador_df, India_df,                      Libya_df, South_Africa_df])

    actual_filename = './Data/TimeSeries/Indicators_TimeSeries_Cleaned.csv'
    final_df.to_csv(actual_filename, index=False)
    
    print('Timeseries Wrangling completed and file created!!', '\n')


# In[19]:

def fileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    
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
        if '_Cleaned' in file:
            filenames.append(file)

    if bucket_name in existent:
        print('Bucket already exists!!', '\n')
        print('TimeSeries Cleaned File upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'TimeSeries/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('TimeSeries CLeaned File uploaded to s3!!!!!','\n')
            
    else:
        print('Bucket not present. Created bucket!!', '\n')
        conn.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        print('TimeSeries CLeaned File upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'TimeSeries/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('TimeSeries Cleaned File uploaded to s3!!!!!','\n')


# In[ ]:

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
    
    readFile()
    writeFile()
    fileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    
    print('Timeseries Wrangling Process completed!!','\n')


# In[ ]:

if __name__ == '__main__':
    main()

