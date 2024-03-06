#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 03 09:11:14 2023

@author: shaunhowe
"""

import os
import glob
import pandas as pd
import datetime

from correct_purple_pm25 import CorrectPurpleAir

def get_sensor_info(sensor_name):
    ''' Function to get sensor id based on sensor name
    '''
    
    #### Determine sensor ID from sensor name
    if sensor == '4_st':
        sensor_id = '144020'
    elif sensor == 'ECA_1':
        sensor_id = '156089'
    elif sensor == 'ECA_2':
        sensor_id = '156193'
    elif sensor == 'ECA_3':
        sensor_id = '156301'
    elif sensor == 'V_st':
        sensor_id = '175119'
    else:
        print('NO SENSOR ID')
        exit()
        
    return sensor_id

def get_files(in_path, start_date, end_date):
    ''' Find PurpleAir CSV files based on start and end date 
    '''
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end   = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    files_between = []
    while start <= end:
        fday = start.strftime('%Y-%m-%d')
        file_path = os.path.join(in_path, f'*{fday}*')
        glob_files = glob.glob(file_path)
        if len(glob_files) == 1:
            files_between.append(glob_files[0])
            
        start += datetime.timedelta(1)
        
    return files_between
    
    
def run_purple_air_correction(input_files, tz):
    ''' Function to run PurpleAir PM2.6 corrections
    '''
    
    #### Correct PurpleAir data
    pa = CorrectPurpleAir(input_files)

    pa.load_data()
    pa.remove_pm25_outliers()
    pa.calculate_mean(f'datetime_{tz}', completeness=complete)
    pa.apply_correction_model(pa.avg_data_hour)
    pa.apply_correction_model(pa.avg_data_day)
    
    return pa

def save_hour_csv(purple_data, out_hour_fn):
    ''' Function to save out hourly PM2.5 data
    '''
    
    #### Save hourly average data
    for day, day_df in purple_data.avg_data_hour.groupby(pd.Grouper(freq='D')):
        print(day.date())
        out_fn = os.path.join(out_hour_fn, f'{sensor_id}_{day.date()}.csv')
        day_df.to_csv(out_fn, index=True, date_format='%Y-%m-%d %H:%M:%S')

def save_day_csv(purple_data, out_day_fn):
    ''' Function to save out daily PM2.5 data
    '''
    if os.path.exists(out_day_fn) == True:
        in_df = pd.read_csv(out_day_fn)
    
    purple_data.avg_data_day = purple_data.avg_data_day.reset_index()

    ## Fix datetimes to match with what's in CSV
    date_day_str = []
    for i in range(len(purple_data.avg_data_day)):
        date_day_str.append(purple_data.avg_data_day[f'datetime_{tz}'].iloc[i].strftime('%Y-%m-%d %H:%M:%S'))

    purple_data.avg_data_day.drop(columns=[f'datetime_{tz}'])
    purple_data.avg_data_day[f'datetime_{tz}'] = date_day_str

    ## Append to daily averaged file if the file exists
    if os.path.exists(out_day_fn) == True:
        in_df = pd.read_csv(out_day_fn)
    
        ## Combine old and new dataframes
        new_day_df = in_df.append(purple_data.avg_data_day)

        ## write daily acerages out to CSV
        new_day_df.to_csv(out_day_fn, index=False, date_format='%Y-%m-%d %H:%M:%S')
    
    ## Create new file if the daily average file doesn't exist
    else:
        ## write daily acerages out to CSV
        purple_data.avg_data_day.to_csv(out_day_fn, index=False, date_format='%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    
    sensor = 'V_st'
    folder = 'corrected_data_robust'
    tz = 'utc' #utc or et
    
    sensor_id = get_sensor_info(sensor)


    #### Set folder and completeness threshold
    if folder == 'corrected_data_robust':
        complete = 0.9
    else:
        complete = None
     
    base_path = r'/path/to/data'
    in_path = rf'{base_path}/{sensor}/raw_data'
    out_hour_fn = rf'{base_path}/{sensor}/{folder}/hour_{tz}'
    out_day_fn = rf'{base_path}/{sensor}/{folder}/{sensor_id}_daily_mean_{tz}.csv'
    

    #### Read in hourly input files
    input_files = get_files(in_path, '2023-07-01', '2024-02-17')
    
    
    #### Function to correct PurpleAir data
    purple_air_dat = run_purple_air_correction(input_files, tz)
    
    
    #### Function to save out hourly data
    save_hour_csv(purple_air_dat, out_hour_fn)
    
    
    #### Function to save out daily data
    save_day_csv(purple_air_dat, out_day_fn)

