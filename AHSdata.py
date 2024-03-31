import os
import pandas as pd
import matplotlib as plt
import folium
from geopy.geocoders import Bing
from folium.plugins import HeatMap
import tqdm
import sqlalchemy
import numpy as np
import datetime

temperatures = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_Temps.csv", skipfooter = 3, engine = 'python')

carbon_levels = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_CO2.csv", skipfooter = 3, engine = 'python', index_col=0)
pd.DataFrame.iteritems = pd.DataFrame.items
pd.set_option("display.max.columns", None)
carbon_levels.dropna(axis = 0, inplace = True)
#print(carbon_levels)

Holidays_for_script = [
    datetime.date(2019, 2, 18), #when the data starts - feb break
    datetime.date(2019, 2, 19),
    datetime.date(2019, 2, 20),
    datetime.date(2019, 2, 21),
    datetime.date(2019, 2, 22),
    datetime.date(2019, 4, 15), #april break
    datetime.date(2019, 4, 16),
    datetime.date(2019, 4, 17),
    datetime.date(2019, 4, 18),
    datetime.date(2019, 4, 19),
    datetime.date(2019, 5, 27) #memorial day
]


carbon_levels.index = pd.to_datetime(carbon_levels.index)
print(carbon_levels.index)

time_is_not_weekend = ~np.isin(carbon_levels.index.weekday, [5,6])
carbon_levels_not_weekends = carbon_levels[time_is_not_weekend]

time_is_not_holiday = ~np.isin(carbon_levels_not_weekends.index.date, Holidays_for_script)
carbon_levels_without_holidays = carbon_levels_not_weekends[time_is_not_holiday]

week = carbon_levels_without_holidays.index.to_period('W-MON')
month = carbon_levels_without_holidays.index.to_period('M')
grouped_by_week = carbon_levels_without_holidays.groupby(week) #makes a dataset that groups times by week
grouped_by_month = carbon_levels_without_holidays.groupby(month) #makes a dataset that groups all times by month





#print(carbon_levels.dtypes)

check_base = carbon_levels.drop(columns = carbon_levels.columns[0], axis = 1)

#for column_name, column_content in carbon_levels.iteritems():          # used to detect if sensor is broken + if sensor is detecting values of over 800 ppm or under 400 ppm
#    for row_name, row_content in carbon_levels.iterrows():
#       if ((column_name != '') and (int(carbon_levels.at[row_name, column_name]) > 800) or ((column_name! = '') and (int(carbon_levels.at[row_name, column_name]) < 400))):
#            if(int(carbon_levels.at[row_name, column_name]) == 2000):
#                print(str(column_name) + " is broken (value is 2000)")
#            else:
#                print(str(row_name) + ' ' + str(column_name) + " recorded a level of " + str(carbon_levels.at[row_name, column_name]) + " ppm.")


print("Weekly values: " + str(grouped_by_week['Cafe UV10 ZN08 CO2'].mean())) #prints out the weekly averages (note this includes weekends too)
print(grouped_by_month['Cafe UV10 ZN08 CO2'].mean()) #prints out monthly averages




                

