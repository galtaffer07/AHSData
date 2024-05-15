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

#LIST OF DATASETS
#r"C:\Users\galta\Downloads\AHS_2019_CO2.csv"
#r"C:\Users\galta\Downloads\2023 Q3Q4 AHS ZN-T & CO2 UTC.csv"
#r"C:\Users\galta\Downloads\2023 Q1Q2 AHS ZN-T & CO2 UTC.csv"


temperatures = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_Temps.csv", skipfooter = 3, engine = 'python')

carbon_levels = pd.read_csv(r"C:\Users\galta\Downloads\2023 Q3Q4 AHS ZN-T & CO2 UTC.csv", skipfooter = 3, engine = 'python', index_col=0) 
carbon_levels_without_empty_columns = carbon_levels[[c for c in carbon_levels.columns if not c.startswith('Unnamed: ')]]  #removes the 175 empty columns at end of dataset
pd.DataFrame.iteritems = pd.DataFrame.items
pd.set_option("display.max.columns", None)
#carbon_levels.dropna(axis = 1, inplace = True)
carbon_levels_without_empty_columns.dropna(axis = 0)


#PROCESS FOR FILTERING
Holidays_for_script = [
    datetime.date(2024, 1, 1),
    datetime.date(2024, 1, 15),
    datetime.date(2024, 2, 19),
    datetime.date(2024, 2, 20),
    datetime.date(2024, 2, 21),
    datetime.date(2024, 2, 22),
    datetime.date(2024, 2, 23),
    datetime.date(2024, 4, 15),
    datetime.date(2024, 4, 16),
    datetime.date(2024, 4, 17),
    datetime.date(2024, 4, 18),
    datetime.date(2024, 4, 19),
    datetime.date(2024, 5, 27),
    datetime.date(2024, 6, 19)
    #datetime.date(2019, 2, 18), #when the data starts - feb break
    #datetime.date(2019, 2, 19),
    #datetime.date(2019, 2, 20),
    #datetime.date(2019, 2, 21),
    #datetime.date(2019, 2, 22),
    #datetime.date(2019, 4, 15), #april break
    #datetime.date(2019, 4, 16),
    #datetime.date(2019, 4, 17),
    #datetime.date(2019, 4, 18),
    #datetime.date(2019, 4, 19),
    #datetime.date(2019, 5, 27) #memorial day
]

early_release_days = [
    datetime.date(2024, 1, 26),
    datetime.date(2024, 3, 8),
    datetime.date(2024, 5, 3)
    #datetime.date(2019, 2, 1),
    #datetime.date(2019, 3, 15),
    #datetime.date(2019, 5, 3)
]
SCHOOL_START_TIME = datetime.time(8, 15) #7:45 for old data, 8:15 for new
NORMAL_RELEASE_TIME = datetime.time(2, 51) #2:20 for old data, 2:51 for new
EARLY_RELEASE_TIME = datetime.time(11, 30) #10:50 for old data, 11:30 for new

carbon_levels_without_empty_columns.index = pd.to_datetime(carbon_levels_without_empty_columns.index)
#print(carbon_levels.index)
#print(carbon_levels.index.time)

time_is_not_notschool = ~np.isin(carbon_levels_without_empty_columns.index.time, (pd.date_range("8:00", "15:00", freq = "1min").time))
carbon_levels_school = carbon_levels_without_empty_columns[time_is_not_notschool] #filters out of school hours

time_is_not_weekend = ~np.isin(carbon_levels_school.index.weekday, [5,6]) #filters weekends
carbon_levels_not_weekends = carbon_levels_school[time_is_not_weekend]

time_is_not_holiday = ~np.isin(carbon_levels_not_weekends.index.date, Holidays_for_script) #filters holidays
carbon_levels_without_holidays = carbon_levels_not_weekends[time_is_not_holiday]

is_early_release_day = np.isin(carbon_levels_without_holidays.index.date, early_release_days) #filters early release days
is_within_early_release_hours = (
    is_early_release_day
    & (carbon_levels_without_holidays.index.time >= SCHOOL_START_TIME)
    & (carbon_levels_without_holidays.index.time <= EARLY_RELEASE_TIME)
)
is_within_normal_school_hours = (
    ~is_early_release_day
    & (carbon_levels_without_holidays.index.time >= SCHOOL_START_TIME)
    & (carbon_levels_without_holidays.index.time <= NORMAL_RELEASE_TIME)
)
carbon_levels_in_normal_school_hours = carbon_levels_without_holidays[
    (is_within_early_release_hours | is_within_normal_school_hours)
]


week = carbon_levels_without_holidays.index.to_period('W-MON')
month = carbon_levels_without_holidays.index.to_period('M')
grouped_by_week = carbon_levels_without_holidays.groupby(week) #makes a dataset that groups times by week
grouped_by_month = carbon_levels_without_holidays.groupby(month) #makes a dataset that groups all times by month


#FINDING RANGE OF DATA TO USE
sorted_column = carbon_levels_without_holidays['Cafe UV08 ZN08 CO2'].sort_values()
first_benchmark = sorted_column.quantile(0.25)
last_benchmark = sorted_column.quantile(0.75)
data_range = last_benchmark - first_benchmark
data_within_range = carbon_levels_without_holidays[(carbon_levels_without_holidays['Cafe UV08 ZN08 CO2'] >= first_benchmark) & (carbon_levels_without_holidays['Cafe UV08 ZN08 CO2'] <= last_benchmark)]

print(str(carbon_levels_without_holidays['CC RTU06 ZN-T'].quantile(0.25)) + "-" + str(carbon_levels_without_holidays['CC RTU06 ZN-T'].quantile(0.75)))



check_base = carbon_levels.drop(columns = carbon_levels.columns[0], axis = 1)

#for column_name, column_content in carbon_levels_without_holidays.iteritems():          # used to detect if sensor is broken + if sensor is detecting values of over 800 ppm
#    for row_name, row_content in carbon_levels_without_holidays.iterrows():
#       if ((carbon_levels_without_holidays.at[row_name, column_name]) > 800) or (((carbon_levels_without_holidays.at[row_name, column_name]) < 400) and ((carbon_levels_without_holidays.at[row_name, column_name]) > 100)):
#           if((carbon_levels_without_holidays.at[row_name, column_name]) == 2000):
#                print(str(column_name) + " is broken (value is 2000)")
#           else:
#               print(str(row_name) + ' ' + str(column_name) + " recorded a level of " + str(carbon_levels_without_holidays.at[row_name, column_name]) + " ppm.")

#print("Weekly values: " + str(grouped_by_week['Cafe UV08 ZN08 CO2'].mean())) 
#print(grouped_by_month['Cafe UV08 ZN08 CO2'].mean()) #prints out monthly averages

v = "ewro"
terminate_loop = False

while not terminate_loop:
    column_name = input("Enter a column name (TEMPERATURE) to check if its system is all-set: ")
    if column_name in carbon_levels_without_holidays.columns:
        if ((abs(carbon_levels_without_holidays[column_name].quantile(0.15))-66 < 2) and (abs(carbon_levels_without_holidays[column_name].quantile(0.85))- 69 < 2)):
            print("Good system")
            print("\nThis system runs from " + str(carbon_levels_without_holidays[column_name].quantile(0.15)) + " F to " + str(carbon_levels_without_holidays[column_name].quantile(0.85)) + " F.")
            while v != "yes":
                v = input("Check again? ").lower()
                if v == "no":
                    terminate_loop = True
                    break
                elif v != "yes":
                    print("Invalid, try again: ")

        else:
            print("Bad system")
            print("\nThis system runs from " + str(carbon_levels_without_holidays[column_name].quantile(0.15)) + " F to " + str(carbon_levels_without_holidays[column_name].quantile(0.85)) + " F.")
            while v != "yes":
                v = input("Check again? ").lower()
                if v == "no":
                    terminate_loop = True
                    break 
                elif v != "yes":
                    print("Invalid, try again: ")

        

