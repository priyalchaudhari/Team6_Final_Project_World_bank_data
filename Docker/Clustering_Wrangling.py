
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import os
pd.options.mode.chained_assignment = None
import numpy as np
import boto3
from boto3.s3.transfer import S3Transfer
import sys


# In[5]:

def readFile():

    homepath = os.path.expanduser('~')
    indicator_data = pd.read_csv('./Data/Clustering/Indicators_Clustering_Combined.csv',                              low_memory=False)
    return indicator_data


# # Handling Missing values for Australia

# In[25]:

def australia():
    
    indicator_data = readFile()

    australia_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                    (indicator_data['CountryCode'] == 'AU')]
    australia_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                     (indicator_data['CountryCode'] == 'AU')]


    australia_df_ind1['Value'] = australia_df_ind1['Value'].fillna(method='bfill', axis = 0)

    australia_df_ind2['Value'] = australia_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Argentina Dataframes

    australia_df = pd.concat([australia_df_ind1, australia_df_ind2, australia_df_ind3, australia_df_ind4, australia_df_ind5,                          australia_df_ind6, australia_df_ind7, australia_df_ind8, australia_df_ind9, australia_df_ind10])
    
    print('Clustering Wrangling completed for Australia!!', '\n')
    
    return australia_df


# # Handling Missing values for Canada

# In[26]:

def canada():
    
    indicator_data = readFile()

    canada_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'CA')]
    canada_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'CA')]


    canada_df_ind1['Value'] = canada_df_ind1['Value'].fillna(method='bfill', axis = 0)

    canada_df_ind2['Value'] = canada_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Brazil Dataframes

    canada_df = pd.concat([canada_df_ind1, canada_df_ind2, canada_df_ind3, canada_df_ind4, canada_df_ind5,                       canada_df_ind6, canada_df_ind7, canada_df_ind8, canada_df_ind9, canada_df_ind10])
    
    print('Clustering Wrangling completed for Canada!!', '\n')
    
    return canada_df


# # Handling Missing values for Saudi Arabia

# In[27]:

def saudi_Arabia():
    
    indicator_data = readFile()

    saudi_arabia_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                  (indicator_data['CountryCode'] == 'SA')]
    saudi_arabia_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'SA')]

    saudi_arabia_df_ind1['Value'] = saudi_arabia_df_ind1['Value'].fillna(method='bfill', axis = 0)

    saudi_arabia_df_ind2['Value'] = saudi_arabia_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Ecuador Dataframes

    saudi_arabia_df = pd.concat([saudi_arabia_df_ind1, saudi_arabia_df_ind2, saudi_arabia_df_ind3, saudi_arabia_df_ind4,                              saudi_arabia_df_ind5, saudi_arabia_df_ind6, saudi_arabia_df_ind7, saudi_arabia_df_ind8,                              saudi_arabia_df_ind9, saudi_arabia_df_ind10])
    
    print('Clustering Wrangling completed for Saudi Arabia!!', '\n')
    
    return saudi_arabia_df


# # Handling Missing values for United States

# In[28]:

def united_States():
    
    indicator_data = readFile()

    united_states_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                        (indicator_data['CountryCode'] == 'US')]
    united_states_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                         (indicator_data['CountryCode'] == 'US')]

    united_states_df_ind1['Value'] = united_states_df_ind1['Value'].fillna(method='bfill', axis = 0)

    united_states_df_ind2['Value'] = united_states_df_ind2['Value'].fillna(method='bfill', axis = 0)

    united_states_df_ind4['Value'] = united_states_df_ind4['Value'].fillna(method='bfill', axis = 0)

    united_states_df_ind5['Value'] = united_states_df_ind5['Value'].fillna(method='bfill', axis = 0)

    united_states_df_ind10['Value'] = united_states_df_ind10['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Libya Dataframes

    united_states_df = pd.concat([united_states_df_ind1, united_states_df_ind2, united_states_df_ind3, united_states_df_ind4,                               united_states_df_ind5, united_states_df_ind6, united_states_df_ind7, united_states_df_ind8,                               united_states_df_ind9, united_states_df_ind10])
    
    print('Clustering Wrangling completed for United States!!', '\n')
    
    return united_states_df


# # Handling Missing values for India

# In[10]:

def india():
    
    indicator_data = readFile()

    india_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'IN')]
    india_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IN')]

    india_df_ind1['Value'] = india_df_ind1['Value'].fillna(method='bfill', axis = 0)

    india_df_ind2['Value'] = india_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the India Dataframes

    india_df = pd.concat([india_df_ind1, india_df_ind2, india_df_ind3, india_df_ind4, india_df_ind5,                      india_df_ind6, india_df_ind7, india_df_ind8, india_df_ind9, india_df_ind10])
    
    print('Clustering Wrangling completed for India!!', '\n')
    
    return india_df


# # Handling Missing values for Russia

# In[11]:

def russia():
    
    indicator_data = readFile()

    russia_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                 (indicator_data['CountryCode'] == 'RU')]
    russia_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'RU')]

    russia_df_ind1['Value'] = russia_df_ind1['Value'].fillna(method='bfill', axis = 0)

    russia_df_ind2['Value'] = russia_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    russia_df = pd.concat([russia_df_ind1, russia_df_ind2, russia_df_ind3, russia_df_ind4, russia_df_ind5, russia_df_ind6,                        russia_df_ind7, russia_df_ind8, russia_df_ind9, russia_df_ind10])
    
    print('Clustering Wrangling completed for Russia!!', '\n')
    
    return russia_df


# # Handling Missing values for South Africa

# In[12]:

def south_Africa():
    
    indicator_data = readFile()

    south_africa_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                       (indicator_data['CountryCode'] == 'ZA')]
    south_africa_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                       (indicator_data['CountryCode'] == 'ZA')]

    south_africa_df_ind1['Value'] = south_africa_df_ind1['Value'].fillna(method='bfill', axis = 0)

    south_africa_df_ind2['Value'] = south_africa_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    south_africa_df = pd.concat([south_africa_df_ind1, south_africa_df_ind2, south_africa_df_ind3, south_africa_df_ind4,                             south_africa_df_ind5, south_africa_df_ind6, south_africa_df_ind7, south_africa_df_ind8,                              south_africa_df_ind9, south_africa_df_ind10])
    
    print('Clustering Wrangling completed for South Africa!!', '\n')
    
    return south_africa_df


# # Handling Missing values for Turkey

# In[13]:

def turkey():
    
    indicator_data = readFile()

    turkey_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                 (indicator_data['CountryCode'] == 'TR')]
    turkey_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'TR')]

    turkey_df_ind1['Value'] = turkey_df_ind1['Value'].fillna(method='bfill', axis = 0)

    turkey_df_ind2['Value'] = turkey_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    turkey_df = pd.concat([turkey_df_ind1, turkey_df_ind2, turkey_df_ind3, turkey_df_ind4, turkey_df_ind5, turkey_df_ind6,                        turkey_df_ind7, turkey_df_ind8, turkey_df_ind9, turkey_df_ind10])
    
    print('Clustering Wrangling completed for Turkey!!', '\n')
    
    return turkey_df


# # Handling Missing values for Argentina

# In[14]:

def argentina():
    
    indicator_data = readFile()

    argentina_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                    (indicator_data['CountryCode'] == 'AR')]
    argentina_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                     (indicator_data['CountryCode'] == 'AR')]


    argentina_df_ind1['Value'] = argentina_df_ind1['Value'].fillna(method='bfill', axis = 0)

    argentina_df_ind2['Value'] = argentina_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Argentina Dataframes

    argentina_df = pd.concat([argentina_df_ind1, argentina_df_ind2, argentina_df_ind3, argentina_df_ind4, argentina_df_ind5,                          argentina_df_ind6, argentina_df_ind7, argentina_df_ind8, argentina_df_ind9, argentina_df_ind10])
    
    print('Clustering Wrangling completed for Argentina!!', '\n')
    
    return argentina_df


# # Handling Missing values for Brazil

# In[17]:

def brazil():
    
    indicator_data = readFile()

    brazil_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'BR')]
    brazil_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'BR')]

    brazil_df_ind1['Value'] = brazil_df_ind1['Value'].fillna(method='bfill', axis = 0)

    brazil_df_ind2['Value'] = brazil_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the Brazil Dataframes

    brazil_df = pd.concat([brazil_df_ind1, brazil_df_ind2, brazil_df_ind3, brazil_df_ind4, brazil_df_ind5,                       brazil_df_ind6, brazil_df_ind7, brazil_df_ind8, brazil_df_ind9, brazil_df_ind10])
    
    print('Clustering Wrangling completed for Brazil!!', '\n')
    
    return brazil_df


# # Handling Missing values for Mexico

# In[16]:

def mexico():
    
    indicator_data = readFile()

    mexico_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                 (indicator_data['CountryCode'] == 'MX')]
    mexico_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'MX')]

    mexico_df_ind1['Value'] = mexico_df_ind1['Value'].fillna(method='bfill', axis = 0)

    mexico_df_ind2['Value'] = mexico_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    mexico_df = pd.concat([mexico_df_ind1, mexico_df_ind2, mexico_df_ind3, mexico_df_ind4, mexico_df_ind5, mexico_df_ind6,                        mexico_df_ind7, mexico_df_ind8, mexico_df_ind9, mexico_df_ind10])
    
    print('Clustering Wrangling completed for Mexico!!', '\n')
    
    return mexico_df


# # Handling Missing values for France

# In[18]:

def france():
    
    indicator_data = readFile()

    france_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                 (indicator_data['CountryCode'] == 'FR')]
    france_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'FR')]

    france_df_ind1['Value'] = france_df_ind1['Value'].fillna(method='bfill', axis = 0)

    france_df_ind2['Value'] = france_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    france_df = pd.concat([france_df_ind1, france_df_ind2, france_df_ind3, france_df_ind4, france_df_ind5, france_df_ind6,                        france_df_ind7, france_df_ind8, france_df_ind9, france_df_ind10])
    
    print('Clustering Wrangling completed for France!!', '\n')
    
    return france_df


# # Handling Missing values for Germany

# In[19]:

def germany():
    
    indicator_data = readFile()

    germany_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                  (indicator_data['CountryCode'] == 'DE')]
    germany_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                   (indicator_data['CountryCode'] == 'DE')]

    germany_df_ind1['Value'] = germany_df_ind1['Value'].fillna(method='bfill', axis = 0)

    germany_df_ind2['Value'] = germany_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    germany_df = pd.concat([germany_df_ind1, germany_df_ind2, germany_df_ind3, germany_df_ind4, germany_df_ind5, germany_df_ind6,                         germany_df_ind7, germany_df_ind8, germany_df_ind9, germany_df_ind10])
    
    print('Clustering Wrangling completed for Germany!!', '\n')
    
    return germany_df


# # Handling Missing values for Italy

# In[20]:

def italy():
    
    indicator_data = readFile()

    italy_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'IT')]
    italy_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'IT')]

    italy_df_ind1['Value'] = italy_df_ind1['Value'].fillna(method='bfill', axis = 0)

    italy_df_ind2['Value'] = italy_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    italy_df = pd.concat([italy_df_ind1, italy_df_ind2, italy_df_ind3, italy_df_ind4, italy_df_ind5, italy_df_ind6,                       italy_df_ind7, italy_df_ind8, italy_df_ind9, italy_df_ind10])
    
    print('Clustering Wrangling completed for Italy!!', '\n')
    
    return italy_df


# # Handling Missing values for United Kingdom

# In[21]:

def united_kingdom():
    
    indicator_data = readFile()

    united_kingdom_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                         (indicator_data['CountryCode'] == 'GB')]
    united_kingdom_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                          (indicator_data['CountryCode'] == 'GB')]

    united_kingdom_df_ind1['Value'] = united_kingdom_df_ind1['Value'].fillna(method='bfill', axis = 0)

    united_kingdom_df_ind2['Value'] = united_kingdom_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    united_kingdom_df = pd.concat([united_kingdom_df_ind1, united_kingdom_df_ind2, united_kingdom_df_ind3, united_kingdom_df_ind4,                               united_kingdom_df_ind5, united_kingdom_df_ind6, united_kingdom_df_ind7, united_kingdom_df_ind8,                                united_kingdom_df_ind9, united_kingdom_df_ind10])
    
    print('Clustering Wrangling completed for United Kingdom!!', '\n')
    
    return united_kingdom_df


# # Handling Missing values for China

# In[22]:

def china():
    
    indicator_data = readFile()

    china_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'CN')]
    china_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'CN')]

    china_df_ind1['Value'] = china_df_ind1['Value'].fillna(method='bfill', axis = 0)

    china_df_ind2['Value'] = china_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    china_df = pd.concat([china_df_ind1, china_df_ind2, china_df_ind3, china_df_ind4, china_df_ind5, china_df_ind6, china_df_ind7,                       china_df_ind8, china_df_ind9, china_df_ind10])
    
    print('Clustering Wrangling completed for China!!', '\n')
    
    return china_df


# # Handling Missing values for Indonesia

# In[23]:

def indonesia():
    
    indicator_data = readFile()

    indonesia_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                    (indicator_data['CountryCode'] == 'ID')]
    indonesia_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                     (indicator_data['CountryCode'] == 'ID')]

    indonesia_df_ind1['Value'] = indonesia_df_ind1['Value'].fillna(method='bfill', axis = 0)

    indonesia_df_ind2['Value'] = indonesia_df_ind2['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    indonesia_df = pd.concat([indonesia_df_ind1, indonesia_df_ind2, indonesia_df_ind3, indonesia_df_ind4, indonesia_df_ind5,                           indonesia_df_ind6, indonesia_df_ind7, indonesia_df_ind8, indonesia_df_ind9, indonesia_df_ind10])
    
    print('Clustering Wrangling completed for Indonesia!!', '\n')
    
    return indonesia_df


# # Handling Missing values for Japan

# In[24]:

def japan():
    
    indicator_data = readFile()

    japan_df_ind1 = indicator_data[(indicator_data['IndicatorCode'].isin(['AG.LND.AGRI.ZS'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind2 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.DYN.CBRT.IN'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind3 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.DPND'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind4 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.EXP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind5 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.IMP.GNFS.ZS'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind6 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.CD'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind7 = indicator_data[(indicator_data['IndicatorCode'].isin(['NY.GDP.MKTP.KD.ZG'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind8 = indicator_data[(indicator_data['IndicatorCode'].isin(['SP.POP.GROW'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind9 = indicator_data[(indicator_data['IndicatorCode'].isin(['FI.RES.TOTL.CD'])) &                                (indicator_data['CountryCode'] == 'JP')]
    japan_df_ind10 = indicator_data[(indicator_data['IndicatorCode'].isin(['NE.TRD.GNFS.ZS'])) &                                 (indicator_data['CountryCode'] == 'JP')]

    japan_df_ind1['Value'] = japan_df_ind1['Value'].fillna(method='bfill', axis = 0)

    japan_df_ind2['Value'] = japan_df_ind2['Value'].fillna(method='bfill', axis = 0)

    japan_df_ind4['Value'] = japan_df_ind4['Value'].fillna(method='bfill', axis = 0)

    japan_df_ind5['Value'] = japan_df_ind5['Value'].fillna(method='bfill', axis = 0)

    japan_df_ind10['Value'] = japan_df_ind10['Value'].fillna(method='bfill', axis = 0)

    # Combining all the South_Africa Dataframes

    japan_df = pd.concat([japan_df_ind1, japan_df_ind2, japan_df_ind3, japan_df_ind4, japan_df_ind5, japan_df_ind6, japan_df_ind7,                       japan_df_ind8, japan_df_ind9, japan_df_ind10])
    
    print('Clustering Wrangling completed for Japan!!', '\n')
    
    return japan_df


# In[45]:

def writeFile():
    
    australia_df = australia()
    canada_df = canada()
    saudi_arabia_df = saudi_Arabia()
    united_states_df = united_States()
    india_df = india()
    russia_df = russia()
    south_africa_df = south_Africa()
    turkey_df = turkey()
    argentina_df = argentina()
    brazil_df = brazil()
    mexico_df = mexico()
    france_df = france()
    germany_df = germany()
    italy_df = italy()
    united_kingdom_df = united_kingdom()
    china_df = china()
    indonesia_df = indonesia()
    japan_df = japan()

    # Combining all countries DataFrame
    final_df = pd.concat([australia_df, canada_df, saudi_arabia_df, united_states_df, india_df, russia_df,                      south_africa_df, turkey_df, argentina_df, brazil_df, mexico_df, france_df, germany_df,                      italy_df, united_kingdom_df, china_df, indonesia_df, japan_df])

    actual_filename = './Data/Clustering/Indicators_Clustering_Cleaned.csv'
    final_df.to_csv(actual_filename, index=False)
    
    print('Clustering Wrangling completed and file created!!', '\n')


# In[ ]:

def fileUploadToS3(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    
    conn = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    transfer = S3Transfer(conn)

    response = conn.list_buckets()    
    existent = []
    for bucket in response["Buckets"]:
        existent.append(bucket['Name'])

    bucket_name = 'Team6FinalProject'
    target_dir = './Data/Clustering/'
    filenames = []
    file_list = os.listdir(target_dir)
    for file in file_list:
        if '_Cleaned' in file:
            filenames.append(file)

    if bucket_name in existent:
        print('Bucket already exists!!', '\n')
        print('Clustering Cleaned File upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'Clustering/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('Clustering CLeaned File uploaded to s3!!!!!','\n')
            
    else:
        print('Bucket not present. Created bucket!!', '\n')
        conn.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        print('Clustering CLeaned File upload started to s3!!!!!', '\n')
        for files in filenames:
            upload_filename = 'Clustering/'+files
            transfer.upload_file(os.path.join(target_dir, files), bucket_name, upload_filename,                                  extra_args={'ACL': 'public-read'})
        print('Clustering Cleaned File uploaded to s3!!!!!','\n')


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
    
    print('Clustering Wrangling Process completed!!','\n')


# In[ ]:

if __name__ == '__main__':
    main()

