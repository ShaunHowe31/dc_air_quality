"""
Created on Sun Jul 16 10:46:43 EDT 2023

@author: shaunhowe
"""

import glob
import os
import pandas as pd
import datetime
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

from plot_purple import plot_colocation

def combine_dataframes(doee_df, df_list):
    ''' Function for combining PM2.5 dataframes
            Used for initial testin
    '''
    
    doee_df = doee_df.rename(columns={'dtime':'datetime_utc'})
    
    ## Merge all PurpleAir dataframes together 
    df_orig = df_list[0][['datetime_utc', 'pm25_ab_avg']]
    df_orig.rename(columns={'pm25_ab_avg':'pm25_ab_avg_1'}, inplace=True)
    
    df_merged = df_orig.merge(df_list[1][['datetime_utc','pm25_ab_avg']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_avg':'pm25_ab_avg_2'}, inplace=True)
    
    df_merged = df_merged.merge(df_list[2][['datetime_utc','pm25_ab_avg']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_avg':f'pm25_ab_avg_3'}, inplace=True)
    
    df_merged = df_merged.merge(df_list[3][['datetime_utc','pm25_ab_avg']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_avg':f'pm25_ab_avg_4'}, inplace=True)
    
    ## Merge combined PurpleAir data into DOEE dataframe
    doee_merged = doee_df.merge(df_merged[['datetime_utc','pm25_ab_avg_1', 'pm25_ab_avg_2', 'pm25_ab_avg_3', 
                                           'pm25_ab_avg_4']], on='datetime_utc', how='outer')
        
    return doee_merged


def combine_corrected_dataframes(doee_df, df_list):
    ''' Function for combining PM2.5 dataframes using previously corrected PurpleAir
            Data. Used for initial testing
    '''
    
    doee_df = doee_df.rename(columns={'dtime':'datetime_utc'})
    
    ## Merge all PurpleAir dataframes together 
    df_orig = df_list[0][['datetime_utc', 'pm25_ab_corrected']]
    df_orig.rename(columns={'pm25_ab_corrected':'pm25_ab_corrected_1'}, inplace=True)
    
    df_merged = df_orig.merge(df_list[1][['datetime_utc','pm25_ab_corrected']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_corrected':'pm25_ab_corrected_2'}, inplace=True)
    
    df_merged = df_merged.merge(df_list[2][['datetime_utc','pm25_ab_corrected']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_corrected':f'pm25_ab_corrected_3'}, inplace=True)
    
    df_merged = df_merged.merge(df_list[3][['datetime_utc','pm25_ab_corrected']], on='datetime_utc', how='outer')
    df_merged.rename(columns={'pm25_ab_corrected':f'pm25_ab_corrected_4'}, inplace=True)
    
    ## Merge combined PurpleAir data into DOEE dataframe
    doee_merged = doee_df.merge(df_merged[['datetime_utc','pm25_ab_corrected_1', 'pm25_ab_corrected_2', 'pm25_ab_corrected_3', 
                                           'pm25_ab_corrected_4']], on='datetime_utc', how='outer')
        
    return doee_merged

def fix_doee_time(dtime):
    ''' Function applied to the DOEE time data to properly convert them to timestamps
    '''
    
    if len(dtime) < 8:
        dtime_good = datetime.datetime.strptime(dtime, '%m/%d/%y')
    else:
        dtime_good = datetime.datetime.strptime(dtime, '%m/%d/%y %H:%M')

    return dtime_good


class AnalyzeColocation():
    ''' Class for reading, storing, and manipulating the DOEE and PurpleAir PM2.5 data
            and applying a linear correction factor
    '''
    def __init__(self, doee_fn, purple_fn):
        self.doee_fn   = doee_fn
        self.purple_fn = purple_fn
        self.doee_df   = None
        self.purple_df = None
    

    def load_purple_data(self, time_offset):
        ''' Function to load in PurpleAir Data
        '''
        ## Read in hourly CSVs
        file_paths = glob.glob(os.path.join(self.purple_fn, '*.csv'))

        ## Read in a list of CSV files and concatenate into single dataframe
        df_list = [pd.read_csv(file) for file in file_paths]
        purple_df = pd.concat(df_list, ignore_index=True)
        
        ## Convert date column to a timestamp
        purple_df['datetime_utc'] = pd.to_datetime(purple_df['datetime_utc'])
            
        ## Rename columns
        purple_df.rename(columns={'pm2.5_ab_avg':'pm25_ab_avg', 'pm2.5_cf_1_a':'pm25_cf_1_a',              
                              'pm2.5_cf_1_b':'pm25_cf_1_b', 'pm2.5_ab_corrected':'pm25_ab_corrected'}, inplace=True)
    
        ## Apply timedelta
        delta = pd.Timedelta(hours=time_offset)
        purple_df['datetime_utc'] = purple_df['datetime_utc'] - delta

        self.purple_df = purple_df

    def load_doee_data(self):
        ''' Function to laod in DC DOEE CSV data
        '''

        doee_df          = pd.read_csv(self.doee_fn)

        doee_df          = doee_df.rename(columns={"Unnamed: 0":"dtime"})
        doee_df          = doee_df.rename(columns={"NEARROAD PM25LC-1022 001h":"PM25"})
        doee_df          = doee_df.iloc[1:]
        doee_df['PM25']  = doee_df['PM25'].astype(float)

        ## Remove single outlier reading of 100000
        doee_df          = doee_df[doee_df['PM25']<10000]


        ## Fix DOEE timestamp issue
        doee_df['dtime'] = doee_df['dtime'].apply(fix_doee_time)

        ## Convert pressure to mb and temperature to fahrenheit
        doee_df['NEARROAD BARPRESS 001h'] = doee_df['NEARROAD BARPRESS 001h'].astype(float)
        doee_df['NEARROAD BARPRESS 001h'] = doee_df['NEARROAD BARPRESS 001h'].apply(lambda x: x*1.33322)
        doee_df['NEARROAD AT_BAM25 001h'] = doee_df['NEARROAD AT_BAM25 001h'].astype(float)
        doee_df['NEARROAD AT_BAM25 001h'] = doee_df['NEARROAD AT_BAM25 001h'].apply(lambda x: ((x*(9/5))+32))


        doee_df.rename(columns={'NEARROAD BARPRESS 001h':'Baropress_001h', 'NEARROAD RELHUM 001h':'Relhum_001h', 
                        'NEARROAD AT_BAM25 001h':'Temp_001h', 'NEARROAD WSP 001h':'Wsp_001h', 'NEARROAD WDR 001h':'Wdr_001h'}, inplace=True)

        doee_df.Baropress_001h.multiply(1.33322)

        self.doee_df = doee_df
        
    def combine_pm_data(self):
        ''' Function to combine DC DOEE and PurpleAir PM2.5 data
        '''
        
        self.doee_df = self.doee_df.rename(columns={'dtime':'datetime_utc'})
        self.doee_purple_df = self.doee_df.merge(self.purple_df[['datetime_utc','pm25_ab_corrected']], on='datetime_utc', how='outer')
    
        ## Drop rows without data for creating linear model
        self.doee_purple_df.dropna(subset=['PM25', 'pm25_ab_corrected'],
                         inplace=True)
        
        ## Save DOEE and PurpleAir PM2.5 data out to lists
        self.doee_pm     = self.doee_purple_df['PM25'].to_list()
        self.purple_pm   = self.doee_purple_df['pm25_ab_corrected'].to_list()
    

    def get_linear_model(self, plot=True):
        ''' Function for finding the linear model between the DOEE and PurpleAir PM2.5 data
                the function can also create a scatter plot with regression data
        '''    
        
        ## Apply a linear regression model
        self.slope, self.inter, self.rval, self.pval, self.stderr = stats.linregress(self.doee_pm, self.purple_pm)
        self.r_squared = round(self.rval**2,3)

        ## Plot correlation scatter
        if plot == True:
            
            fig, ax = plt.subplots(1,figsize=(6.5,6.5))

            ax.scatter(self.doee_pm, self.purple_pm,marker='o',color='purple', edgecolor='k')
            ax.plot(np.unique(self.doee_pm), np.poly1d(np.polyfit(self.doee_pm, self.purple_pm, 1))(np.unique(self.doee_pm)), color='black')
            ax.grid()
            plt.title('Corrected PurpleAir vs DOEE PM2.5 Sensor',fontsize=16.)
            ax.set_xlabel('DC DOEE PM2.5',fontsize=14.)
            ax.set_ylabel('Corrected PurpleAir PM2.5',fontsize=14.)
            ax.annotate('$R^2$: '+str(round(self.r_squared,2)), xy=(20, 250), fontsize=14)
            ax.annotate('y = '+str(round(self.slope,2))+'x + '+str(round(self.inter,2)), xy=(20, 265), fontsize=14.)


            plt.show()
            
    def correct_purple_pm(self):
        ''' Function for applying the linear regressiom model 
        '''
        
        self.purple_pm_corrected = (np.asarray(self.purple_pm)-self.inter)/self.slope
        

    def plot_correction_comparison(self, plot_title, x_label, y_label):
        ''' Function for plotting the corrected PurpleAir PM2.5 data against the DC DOEE data
        '''
        
        x = range(len(self.doee_pm))
        
        fig, ax = plt.subplots(1,figsize=(9,6))
        
        ax.scatter(x, self.doee_pm, label='DOEE PM')
        ax.plot(x, self.doee_pm, linestyle='--')
        ax.scatter(x, self.purple_pm_corrected, label='Final Correction')
        ax.plot(x, self.purple_pm_corrected, linestyle='--')
        ax.set_xlabel(x_label, fontsize=14.)
        ax.set_ylabel(y_label, fontsize=14.)
        ax.set_title(plot_title, fontsize=18.)
        ax.grid()
        ax.legend()
        
        plt.show()

if __name__ == '__main__':
    
    from bokeh.models import HoverTool

    purple_sensor    = 'ECA_2'
    doee_fn          = '/path/to/doee/data'
    purple_dir       = rf'/path/to/purpleair/data/folder'

    
    doee_pa = AnalyzeColocation(doee_fn, purple_dir)
    doee_pa.load_doee_data()
    doee_pa.load_purple_data(5)
    doee_pa.combine_pm_data()
    doee_pa.get_linear_model(plot=True)
    doee_pa.correct_purple_pm()
    
    doee_pa.plot_correction_comparison('DOEE vs Barkjohn+Linearly Corrected PurpleAir \n 5-Hour Offset Applied', 'Dataset Length', 'PM2.5')
    
