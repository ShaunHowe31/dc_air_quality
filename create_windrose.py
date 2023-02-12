#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 10:26:01 2022

@author: shaunhowe
"""

import glob
import numpy as np
import pandas as pd
from datetime import datetime

from windrose import WindroseAxes

def get_windy_withit(file_list, sigma_thresh=2):
    ''' Analyze yearly wind data
    '''
    
    master_time = np.array([])
    master_windd = np.array([])
    master_winds = np.array([])
    
    for file in file_list:
        year = file.split('_')[-1].split('.')[0]
        print(f'Analyzing met data from {year}')
        
    
        met_data = pd.read_csv(file)
        
        time_str = met_data['DATE'].to_list()
        time_dt = [datetime.strptime(tme, '%Y-%m-%dT%H:%M:%S') for tme in time_str]
        wind = met_data['WND'].to_list()
        
        
        
        split_wind = [i.split(',') for i in wind]
        
        split_wind = np.asarray(split_wind)
        
        raw_windd = split_wind[:,0].astype(float)
        # windd_qc = split_wind[:,1].astype(int)
        # wind_type = split_wind[:,2].astype(float)
        raw_winds = split_wind[:,3].astype(float)/10. ## scaling factor 10
        # winds_qc = split_wind[:,4].astype(int)
        
        ## Find missing wind measurements
        
        missing_windd = np.where(raw_windd==999)[0]
        missing_winds = np.where(raw_winds==9999)[0]
        missing_combined = np.concatenate((missing_windd, missing_winds))
        missing_wind_final = np.unique(missing_combined)
        
        # sus_windd = np.where(windd_qc==2)[0]
        # erron_windd = np.where(windd_qc==3)[0]
        # sus_winds = np.where(winds_qc==2)[0]
        # erron_winds = np.where(winds_qc==3)[0]
        
        ## Remove missing values
        good_windd = np.delete(raw_windd, missing_wind_final)
        good_winds = np.delete(raw_winds, missing_wind_final)
        good_time = np.delete(time_dt, missing_wind_final)
        
        ## Standard deviation filter
        winds_sigma = np.std(good_winds)
        sigma_filt = np.where(good_winds>(sigma_thresh*winds_sigma))[0]
        
        normal_windd = np.delete(good_windd, sigma_filt)
        normal_winds = np.delete(good_winds, sigma_filt)
        normal_time = np.delete(good_time, sigma_filt)
        
        master_time = np.append(master_time, normal_time)
        master_windd = np.append(master_windd, normal_windd)
        master_winds = np.append(master_winds, normal_winds)

    
    return master_time, master_windd, master_winds


def get_seasonal_withit(tme, windd, winds):
    ''' Sort the wind data seasonally
    '''
    
    winter_windd = np.array([])
    winter_winds = np.array([])
    spring_windd = np.array([])
    spring_winds = np.array([])
    summer_windd = np.array([])
    summer_winds = np.array([])
    fall_windd = np.array([])
    fall_winds = np.array([])
    
    for i in range(len(tme)):
        month = tme[i].month
        if month in {12,1,2}:
            winter_windd = np.append(winter_windd, windd[i])
            winter_winds = np.append(winter_winds, winds[i])
        elif month in {3,4,5}:
            spring_windd = np.append(spring_windd, windd[i])
            spring_winds = np.append(spring_winds, winds[i])
        elif month in {6,7,8}:
            summer_windd = np.append(summer_windd, windd[i])
            summer_winds = np.append(summer_winds, winds[i])
        elif month in {9,10,11}:
            fall_windd = np.append(fall_windd, windd[i])
            fall_winds = np.append(fall_winds, winds[i])
            
        
    return (winter_windd, winter_winds), (spring_windd, spring_winds), (summer_windd, summer_winds), (fall_windd, fall_winds)
        

def plot_windrose(windd, winds, bins, title):
    ''' Function for plotting a windrose          
    '''
    ## Plot windrose

    ax = WindroseAxes.from_ax()
    # ax.bar(good_windd, good_winds, normed=True, opening=0.8, edgecolor='white')
    ax.bar(windd, winds, normed=True, opening=0.8, bins=bins, edgecolor='white')
    ax.set_legend()
    ax.set_title(title, fontsize=20.)
            


if __name__ == '__main__':

    # fn = r'/Users/shaunhowe/Documents/research/eckington_aq/data/met/cp_awos/72224400358_2011.csv'
    # file_path = r'/Users/shaunhowe/Documents/research/eckington_aq/data/met/cp_awos/*.csv'
    file_path = r'/Users/shaunhowe/Documents/research/eckington_aq/data/met/dca_asos/*.csv'
    # file_path = r'/Users/shaunhowe/Documents/research/eckington_aq/data/met/phx_asos/*.csv'
    


    file_list = glob.glob(file_path)
    sigma = 2.0
    
    tme, windd, winds = get_windy_withit(file_list, sigma_thresh=sigma)
    
    winter, spring, summer, fall = get_seasonal_withit(tme, windd, winds)
    
    bins = np.arange(1,5,.5)
    
    plot_windrose(fall[0], fall[1], bins, 'Reagan National Airport Wind Rose \n Fall - 2007 to 2022')
    # plot_windrose(windd, winds, bins, 'Reagan National Airport Wind Rose \n 2007 to 2022')

    