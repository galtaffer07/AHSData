import os
import pandas as pd
import matplotlib as plt
import folium
from geopy.geocoders import Bing
from folium.plugins import HeatMap
import tqdm
import sqlalchemy

temperatures = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_Temps.csv", skipfooter = 3, engine = 'python')

carbon_levels = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_CO2.csv", skipfooter = 3, engine = 'python', index_col=0)
pd.DataFrame.iteritems = pd.DataFrame.items
pd.set_option("display.max.columns", None)
carbon_levels.dropna(axis = 0, inplace = True)
#print(carbon_levels)

carbon_levels.index = pd.to_datetime(carbon_levels.index)
week = carbon_levels.index.to_period('W-MON')
grouped_by_week = carbon_levels.groupby(week)



#print(carbon_levels.dtypes)

check_base = carbon_levels.drop(columns = carbon_levels.columns[0], axis = 1)

#for column_name, column_content in carbon_levels.iteritems():
#    for row_name, row_content in carbon_levels.iterrows():
#        if ((column_name != '') and (int(carbon_levels.at[row_name, column_name]) > 800)):
#            if(int(carbon_levels.at[row_name, column_name]) == 2000):
#                print(str(column_name) + " is broken (value is 2000)")
#            else:
#                print(str(row_name) + ' ' + str(column_name) + " recorded a level of " + str(carbon_levels.at[row_name, column_name]) + " ppm.")


print(grouped_by_week['Cafe UV10 ZN08 CO2'].mean())


                

