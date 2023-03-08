#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 29 15:22:42 2022

@author: shaunhowe
"""

import numpy as np
import pandas as pd
from datetime import datetime


class CorrectPurpleAir():
    '''
    '''
    def __init__(self, fn):
        self.filenames = fn
        

    def load_data(self):
        ''' Load CSV data into dataframe
        '''
        
        print('Loading in Data')
        
        df_all = []
        
        ## loop through CSVs
        for file in self.filenames:
            
            ## Read in sensor data
            raw_data = pd.read_csv(file)

            ## Convert Unix timestamp to datetime then make timezone eastern for daily averaging
            raw_data['datetime_utc'] = pd.to_datetime(raw_data['time_stamp'], unit='s', utc=True)
            raw_data['datetime_et'] = raw_data['datetime_utc'].dt.tz_convert('US/Eastern')
            
            df_all.append(raw_data)

        self.raw_data = pd.concat(df_all)

        
        ## call class to automatically remove lines with bad met data
        self.remove_bad_met()
        
        
    def remove_bad_met(self):
        ''' Remove erroneous high and low T and RH
                T above 540C 
                RH above 100%
        '''
        
        print('Removing bad met')
        
        ## Filter out erroneous temperature and humidity values
        self.filter_data = self.raw_data[~(self.raw_data['temperature'] >= 1000.)]  
        self.filter_data = self.raw_data[~(self.raw_data['humidity'] > 100.)]
        
        
    def remove_pm25_outliers(self):
        ''' Remove PM2.5 Outliers
                Remove PM2.5 values where percent different larger than 2 SD (61%)
                    Used the same method for relative difference as Barkjohn et al 2021
                Remove PM2.5 values where different is greater than 5 micrograms
        '''
        
        print('Removing PM2.5 outliers')
        
        pm25_a = self.filter_data['pm2.5_cf_1_a'].to_numpy()
        pm25_b = self.filter_data['pm2.5_cf_1_b'].to_numpy()

        ## Percent difference filtering
        pct_diff = (np.abs(pm25_a-pm25_b)*2)/(pm25_a+pm25_b)
        pm25_a_filt = np.where(pct_diff>(2*np.nanstd(pct_diff)), np.nan, pm25_a)
        pm25_b_filt = np.where(pct_diff>(2*np.nanstd(pct_diff)), np.nan, pm25_b)

        ## Difference filtering (only for 24 hour averages)
        ## This condition was used only on the 24 hour averages
#        pm25_a_filt = np.where(np.abs(pm25_a_filt-pm25_b_filt)>5, np.nan, pm25_a_filt)
#        pm25_b_filt = np.where(np.abs(pm25_a_filt-pm25_b_filt)>5, np.nan, pm25_b_filt)

        ## Put filtered data back into dictionary
        self.filter_data.insert(0, 'pm2.5_filt_a', pm25_a_filt, True)
        self.filter_data.insert(1, 'pm2.5_filt_b', pm25_b_filt, True)

#        ## Drop columns with NaN values
        self.filter_data = self.filter_data.dropna(subset=['pm2.5_filt_a', 'pm2.5_filt_b', 'humidity'])

        
    def calculate_mean(self, time_var, completeness=None):
        ''' Average A and B channels and perform Hourly and Daily averaging
        '''
        
        print('Calculating PM2.5 A/B channel mean')
        
        ## Drop columns with NaN values
        self.filter_data = self.filter_data.dropna(subset=['pm2.5_filt_a', 'pm2.5_filt_b', 'humidity'])
        
        ## Hourly and Daily averaging
        self.filter_data['pm2.5_ab_avg'] = self.filter_data[['pm2.5_filt_a','pm2.5_filt_b']].mean(axis=1)
        

        print('Calculating PM2.5 daily and hourly mean')
        
        ## Create Hourly and Daily average
        self.avg_data_hour = self.filter_data.resample('H', on=time_var).mean()
        self.avg_data_day = self.filter_data.resample('d', on=time_var).mean()
        
        ## Filter data based on completeness from time resampling
        if completeness != None:
            data_avail_hour = self.filter_data.resample('H', on=time_var).count()/30
            data_avail_day = self.filter_data.resample('d', on=time_var).mean()/720
        
            ## Create time resampling completeness mask
            mask_hour = data_avail_hour >= completeness
            mask_day = data_avail_day >= completeness

            ## Apply time resampling completeness mask
            self.avg_data_hour = self.avg_data_hour[mask_hour]
            self.avg_data_day = self.avg_data_day[mask_day]
            
            ## Filter out masked data
            self.avg_data_hour = self.avg_data_hour.dropna(subset=['pm2.5_ab_avg', 'humidity'])
            self.avg_data_day = self.avg_data_day.dropna(subset=['pm2.5_ab_avg', 'humidity'])
            

        
    def apply_correction_model(self, data_dict):
        ''' Correct PM2.5 counts from Barkjohn et al 2021 model
        '''
        
        print('Calculating corrected PM2.5 values')
        
        pm25_ab = data_dict['pm2.5_ab_avg'].to_numpy()
        rh = data_dict['humidity'].to_numpy()
        
        ## Apply Barkjohn et al 2021 correction model
        pm25_corrected = 0.524*pm25_ab-0.0862*rh+5.75
        
        ## Add corrected PM2.5 data back into data dictionary
        data_dict['pm2.5_ab_corrected'] = pm25_corrected
        

if __name__ == '__main__':
    
    import os
    import glob
    

    in_path = r'/path/to/csv/files'
    
    input_files = glob.glob(os.path.join(in_path,'*.csv'))
    

    pa = CorrectPurpleAir(input_files)
    
    pa.load_data()
    pa.remove_pm25_outliers()
    pa.calculate_mean('datetime_et', completeness=.9)
    pa.apply_correction_model(pa.avg_data_hour)
    pa.apply_correction_model(pa.avg_data_day)

    
