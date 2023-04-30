#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 21:19:00 2022

@author: shaunhowe
"""

import requests
import time
import datetime
import json
import pandas as pd


def get_hist_purple_data(my_api_read_key, sensor_index, starttime, endtime, average_period=0):
    ''' Function for pulling historical purple air data through PurpleAir API
    '''
    
    print('Querying API')
    
    ## Get Unix timestamp for query date/time range
    starttime_unix = time.mktime(starttime.timetuple())
    endtime_unix = time.mktime(endtime.timetuple())

    ## Set API parameters
    parameters = {'average':average_period, 'start_timestamp':starttime_unix,
                  'end_timestamp':endtime_unix, 
                  'fields':'humidity, temperature, pressure, pressure_a, pressure_b,\
                    pm1.0_atm, pm1.0_atm_a, pm1.0_atm_b, pm1.0_cf_1, pm1.0_cf_1_a,\
                    pm1.0_cf_1_b, pm2.5_alt, pm2.5_alt_a, pm2.5_alt_b, pm2.5_atm,\
                    pm2.5_atm_a, pm2.5_atm_b, pm2.5_cf_1, pm2.5_cf_1_a, pm2.5_cf_1_b,\
                    pm10.0_atm, pm10.0_atm_a, pm10.0_atm_b, pm10.0_cf_1, pm10.0_cf_1_a,\
                    pm10.0_cf_1_b, scattering_coefficient, scattering_coefficient_a,\
                    scattering_coefficient_b, deciviews, deciviews_a, deciviews_b, \
                    visual_range, visual_range_a, visual_range_b, rssi, uptime,\
                    pa_latency, memory'}
                      

#    my_url = f'https://api.purpleair.com/v1/sensors/{sensor_index}/history/csv'
    my_url = f'https://api.purpleair.com/v1/sensors/{sensor_index}/history'


    ## Set the API header
    my_headers = {'X-API-Key':my_api_read_key}
    
    #r = requests.get(my_url, headers=my_headers)
    r = requests.get(my_url, headers=my_headers, params=parameters)

    ## Return the response 
    return r

def get_current_purple_data(my_api_read_key, sensor_index):
    ''' Pull current data from PurpleAir sensors
    '''
    my_url = f'https://api.purpleair.com/v1/sensors/{sensor_index}'
    
    parameters = {'fields': 'uptime, date_created, temperature, pm2.5_alt'}
    
    my_headers = {'X-API-Key':my_api_read_key}
    
    r = requests.get(my_url, headers=my_headers, params=parameters)
    
    ## Return the response 
    return r


def save_output(data, sensor_index, data_date, out_path):
    ''' Save PurpleAir JSON dictionary as a CSV
    '''
    
    print('Saving output CSV file')
    
    ## Set up file path and name
    data_date_str = datetime.datetime.strftime(data_date, '%Y-%m-%d')
    file_path = f'{out_path}/{sensor_index}_{data_date_str}.csv'
    
    ## Create Pandas dataframe from PurpleAir JSON dictionary
    fields = data.json()['fields']
    purple_df = pd.DataFrame.from_dict(data.json()['data'])
    purple_df = purple_df.set_axis(fields, axis=1, inplace=False)
    
    ## Sort datafrane by time index since the data are returned out of order
    purple_df = purple_df.sort_values(by=['time_stamp'])
    
    purple_df.to_csv(file_path, index=False)
    

def download_multiple_days(api_read_key, sensor_index, start_date, end_date, out_path, sleep_time=600):
    ''' Function to loop through multiple days to download PurpleAir data
    '''
    
    ## Convert input date strings to datetime objects
    start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    starttime = start_dt 
    while starttime < end_dt:
        endtime = starttime+datetime.timedelta(days=1)
        
        print(f'Getting data for {starttime} to {endtime}')
        
        ## Get data through API
        data = get_hist_purple_data(api_read_key, sensor_index, starttime, endtime)
        
        ## Save output data JSON to CSV
        if len(data.json()['data']) != 0:
            save_output(data, sensor_index, starttime, out_path)
        
        time.sleep(sleep_time)
        
        starttime = endtime
        

if __name__ == '__main__':
    
    out_path = r'/path/to/output/files'
    download_multiple_days(my_api_read_key, sensor_id, '2022-11-01', '2022-12-01', out_path, sleep_time=210)
