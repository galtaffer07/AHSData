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
from meteostat import Hourly, Daily, Point


#LIST OF DATASETS
#r"C:\Users\galta\Downloads\AHS_2019_CO2.csv"
#r"C:\Users\galta\Downloads\2023 Q3Q4 AHS ZN-T & CO2 UTC.csv"
#r"C:\Users\galta\Downloads\2023 Q1Q2 AHS ZN-T & CO2 UTC.csv"
#print(f"Pandas version: {pd.__version__}")

temperatures = pd.read_csv(r"C:\Users\galta\Downloads\AHS_2019_Temps.csv", skipfooter = 3, engine = 'python')

carbon_levels = pd.read_csv(r"C:\Users\galta\Downloads\2023 Q3Q4 AHS ZN-T & CO2 UTC.csv", skipfooter = 3, engine = 'python', index_col=0) 
pd.set_option('display.max_rows', 10, 'display.max_columns', None)
#print(carbon_levels)
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
carbon_levels_school = carbon_levels_without_empty_columns[~time_is_not_notschool] #filters out of school hours


time_is_not_weekend = ~np.isin(carbon_levels_school.index.weekday, [5,6]) #filters weekends
carbon_levels_not_weekends = carbon_levels_school[time_is_not_weekend]

#print(carbon_levels_not_weekends)

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

#print(str(carbon_levels_without_holidays['CC RTU06 ZN-T'].quantile(0.25)) + "-" + str(carbon_levels_without_holidays['CC RTU06 ZN-T'].quantile(0.75)))


check_base = carbon_levels.drop(columns = carbon_levels.columns[0], axis = 1)


#for column_name, column_content in carbon_levels_without_holidays.iteritems():          # used to detect if sensor is broken + if sensor is detecting values of over 800 ppm
#    for row_name, row_content in carbon_levels_without_holidays.iterrows():
#       if (((carbon_levels_without_holidays.at[row_name, column_name]) > 800) | (((carbon_levels_without_holidays.at[row_name, column_name]) < 400) & ((carbon_levels_without_holidays.at[row_name, column_name]) > 100))):
#           if((carbon_levels_without_holidays.at[row_name, column_name]) == 2000):
#                print(str(column_name) + " is broken (value is 2000)")
#           else:
#               print(str(row_name) + ' ' + str(column_name) + " recorded a level of " + str(carbon_levels_without_holidays.at[row_name, column_name]) + " ppm.")

#print("Weekly values: " + str(grouped_by_week['RM309 ZN14 Q CO2'].mean())) 
#print(grouped_by_month['RM309 ZN14 Q CO2'].mean()) #prints out monthly averages
'''
v = "input"
terminate_loop = False
targ1 = 70
targ2 = 80   #please change as needed

def flag_sensors(df, min_level=1600, max_level=1900):
    flagged_sensors = []
    for sensor in df.columns:
        if df[sensor].between(min_level, max_level).any():
            flagged_sensors.append(sensor)
    return flagged_sensors

def sort_sensors(df, min_level=1600, max_level=1900):
    high = []
    medium = []
    light = []

    for sensor in df.columns:
        count = df[sensor].between(min_level, max_level).sum()
        if count >= 50:
            high.append(sensor)
        elif 5 <= count < 25:
            medium.append(sensor)
        elif count > 0:
            light.append(sensor)

    return high, medium, light

def broken_sensors(df, min_level=1900, max_level=5000):
    broken_sensors = []
    for sensor in df.columns:
        if df[sensor].between(min_level, max_level).any():
            broken_sensors.append(sensor)
        return broken_sensors

def find_rooms_needing_AC(df):
    sensor_medians={}

    for sensor in df.columns:
        top_temps = df[sensor].nlargest(21)
        sensor_medians[sensor] = top_temps.median()

    sorted_sensors = sorted(sensor_medians.items(), key=lambda x: x[1], reverse = True)
    return sorted_sensors

temperature = [col for col in carbon_levels_without_holidays.columns if 'CO2' not in col.upper()]#checks to see if columns in dataset do not have CO2 in name => temperature sensor
print(find_rooms_needing_AC(carbon_levels_without_holidays[temperature]))'''

carbon_levels_without_holidays['date'] = carbon_levels_without_holidays.index.date

temp_columns=[col for col in carbon_levels_without_holidays.columns if "ZN-T" in col and "Boiler" not in col and "Outside" not in col and "AHU" not in col and "Field" not in col and "CC" not in col]

carbon_levels_without_holidays.fillna(method='ffill', inplace=True)
#METHOD 5 - DISCOMFORT FACTOR
comfort_threshold = 78
room_discomfort_data = []

for col in temp_columns:
    room_temps = carbon_levels_without_holidays[col]

    # Count how many readings are above the threshold
    time_above_threshold = (room_temps > comfort_threshold).sum()

    # Get max temperature reached in the room
    max_temp = room_temps.max()

    # Get time taken to cool back below threshold (if possible)
    above_threshold_indices = room_temps[room_temps > comfort_threshold].index
    if len(above_threshold_indices) > 0:
        first_above_time = above_threshold_indices.min()
        below_threshold_indices = room_temps[room_temps <= comfort_threshold].index
        time_to_cool = below_threshold_indices[below_threshold_indices > first_above_time].min() - first_above_time if len(below_threshold_indices) > 0 else None
    else:
        time_to_cool = None  # Never cooled below threshold

    room_discomfort_data.append({
        'Room': col,
        'Time Above 78°F': time_above_threshold,
        'Max Temp': max_temp,
        'Time to Cool Below 78°F': time_to_cool if time_to_cool is not None else float('inf')
    })
discomfort_df = pd.DataFrame(room_discomfort_data)
discomfort_df['Time to Cool Below 78°F'] = discomfort_df['Time to Cool Below 78°F'].apply(
    lambda x: x.total_seconds() / 3600 if isinstance(x, pd.Timedelta) else (9999 if pd.isna(x) else x)
)
first_above_time = pd.to_datetime(first_above_time)
below_threshold_indices = pd.to_datetime(below_threshold_indices)




discomfort_df = discomfort_df.sort_values(by=['Time Above 78°F', 'Max Temp', 'Time to Cool Below 78°F'], ascending=[False, False, True])
print(discomfort_df.head(6))

#latitude = 42.6572
#longitude = -71.1555
#location_point = Point(latitude, longitude)


def count_high_indoor_temps(data, latitude=42.6572, longitude=-71.1555):
    """
    Counts how many times each sensor recorded above 78°F when outdoor temperature was above 80°F.

    Parameters:
    - data (pd.DataFrame): Dataset with timestamps as index and sensor names as columns.
    - latitude (float): Latitude of the location for outdoor temperature data.
    - longitude (float): Longitude of the location for outdoor temperature data.

    Returns:
    - pd.Series: Number of times each sensor exceeded 78°F when outdoor temp > 80°F.
    """

    # Define location
    location_point = Point(latitude, longitude)

    # Fetch outdoor temperatures for dataset time range
    start, end = data.index.min(), data.index.max()
    outdoor_temps = Hourly(location_point, start, end).fetch()

    # Convert temp to Fahrenheit
    outdoor_temps["temp"] = outdoor_temps["temp"] * 9/5 + 32

    # Merge outdoor temperatures into dataset
    data = data.merge(outdoor_temps[["temp"]], left_index=True, right_index=True, how="left")
    data.rename(columns={"temp": "Outdoor Temp"}, inplace=True)

    # Ensure all sensor columns are numeric
    temp_columns = data.select_dtypes(include=["number"]).columns
    temp_columns=[col for col in carbon_levels_without_holidays.columns if "ZN-T" in col and "Boiler" not in col and "Outside" not in col and "AHU" not in col and "Field" not in col and "CC" not in col]

    # Filter for rows where outdoor temp > 80°F
    filtered_data = data[data["Outdoor Temp"] > 82]

    # Drop non-sensor columns before comparison
    filtered_data = filtered_data[temp_columns]

    # Count occurrences where each sensor is above 78°F
    counts = (filtered_data > 82).sum()

    counts = counts.sort_values(ascending=False)

    return counts

high_temp_counts = count_high_indoor_temps(carbon_levels_without_holidays)

# Print results
print(high_temp_counts)



        
# rank rooms by highest average temperature difference
ranked_rooms = sorted(room_temp_diffs.items(), key=lambda x: x[1], reverse=True)

# print top 5 rooms with greatest cooling needs
print("Top 5 Rooms with Greatest Cooling Needs (Avg. Temp Difference °F):")
for room, diff in ranked_rooms[:5]:
    print(f"{room}: {diff:.2f}°F")

'''


#weather_temp_column = 'temperature'
inside_threshold = int(input("Enter a threshold value to check: "))
carbon_levels_without_holidays['date']=carbon_levels_without_holidays.index.date
#threshold = 22
#high_temp_data = carbon_levels_without_holidays[carbon_levels_without_holidays[weather_temp_column] > threshold]

#below script used to contrast room temps with a threshold inputted by user

sensor_count = {}

# Iterate over each temperature column
for col in temp_columns:
    #find rows where this sensor exceeds the threshold
    high_temp_data = carbon_levels_without_holidays[carbon_levels_without_holidays[col] > inside_threshold]

    #check if we have any rows where the temperature exceeds 78F
    if not high_temp_data.empty:
        #only want to print one instance per day
        high_temp_data_daily = high_temp_data.groupby('date')[col].max().reset_index()

        #print the date and the sensor column if the condition is met
        print(f"Sensor: {col}")
        print(high_temp_data_daily[['date', col]])  #only print date and the sensor value
        print("-" * 40)  #separator for clarity

#this loop counts all rows
for col in temp_columns:
    high_temp_data2 = carbon_levels_without_holidays[carbon_levels_without_holidays[col] > inside_threshold]
    days_exceed_count = high_temp_data2['date'].nunique()
    sensor_count[col] = days_exceed_count

sensor_count_df=pd.DataFrame(sensor_count.items(), columns = ['Sensor', 'Number of Days Exceeding ' + str(inside_threshold)])

print(sensor_count_df.sort_values(by = 'Number of Days Exceeding ' + str(inside_threshold), ascending = False))

# DAILY AVG TEMPERATURE
daily_avg_temps = carbon_levels_without_holidays.groupby('date')[temp_columns].mean()

#compute the overall average temperature for each room across all days
overall_avg_temps = daily_avg_temps.mean()

# get the top 5 rooms with the highest overall average temperature
top_5_rooms = overall_avg_temps.nlargest(5)

# print results
print("Top 5 rooms with the highest average temperature across all days:")
print(top_5_rooms)

#TEMP PERSISTENCE
carbon_levels_without_holidays['date'] = carbon_levels_without_holidays.index.date

sensor_persistence_days = {}

for col in temp_columns:
    # Check if any reading exceeds threshold per day
    daily_high = carbon_levels_without_holidays.groupby('date')[col].max() > inside_threshold

    # Identify streaks of consecutive high-temp days
    streaks = daily_high.astype(int).groupby((daily_high != daily_high.shift()).cumsum()).cumsum()
    
    # Find longest streak for this sensor
    max_streak = streaks.max()

    sensor_persistence_days[col] = max_streak

# Sort and print the top 5 longest persistence values (by day)
sorted_persistence_days = sorted(sensor_persistence_days.items(), key=lambda x: x[1], reverse=True)[:5]

print("Top 5 sensors with longest high-temperature persistence (by days):")
for sensor, streak in sorted_persistence_days:
    print(f"{sensor}: {streak} consecutive days over {inside_threshold}°F")

#COOLING EFFICIENCY
cooling_rates = {}

for col in temp_columns:
    temp_drops = []  # Store cooling efficiency values for this sensor

    # Group by date and find max temperature & timestamp
    daily_max = carbon_levels_without_holidays.groupby('date')[col].idxmax()
    
    for timestamp in daily_max:
        if pd.notna(timestamp):  # Ensure valid timestamp
            # Look at temp 2 hours later (adjustable)
            later_time = timestamp + pd.Timedelta(hours=2)

            if later_time in carbon_levels_without_holidays.index: #ensure times exist in dataset
                temp_peak = carbon_levels_without_holidays.loc[timestamp, col]
                temp_later = carbon_levels_without_holidays.loc[later_time, col]


                #ensure they are single values
                if isinstance(temp_peak, pd.Series):
                    temp_peak = temp_peak.iloc[0]
                if isinstance(temp_later, pd.Series):
                    temp_later = temp_later.iloc[0]

                drop = temp_peak - temp_later  # Calculate cooling drop

                if drop > 0:  # Ensure meaningful cooling data
                    temp_drops.append(drop)

    # Store average cooling drop for this sensor
    if temp_drops:
        cooling_rates[col] = sum(temp_drops) / len(temp_drops)

# Sort and display the top 5 sensors with best cooling efficiency
sorted_cooling = sorted(cooling_rates.items(), key=lambda x: x[1], reverse=False)[:5]

print("Top 5 rooms with worst cooling efficiency (Avg. °F drop after peak temp):")
for sensor, drop in sorted_cooling:
    print(f"{sensor}: {drop:.2f}°F after 2 hours")





















indoor_columns = [col for col in high_temp_data.columns if not ('Outside' in col)]

#finds the temperature of the rooms when the outside temp exceeds a certain value (in CELSIUS)
high_temp_data_indoor = high_temp_data[indoor_columns]

temp_columns = [col for col in temp_columns if col in high_temp_data_indoor.columns]

high_temp_data_clean = high_temp_data_indoor.dropna(subset=temp_columns, how='all')

#goes thru each column and tries to locate the hottest temp and prints that out
high_temp_data_clean.loc[:,'hottest_room'] = high_temp_data_clean[temp_columns].idxmax(axis = 1)
high_temp_data_clean.loc[:,'max_temperature'] = high_temp_data_clean[temp_columns].max(axis = 1)

print(high_temp_data_clean[['hottest_room', 'max_temperature', weather_temp_column]])








#print(find_max_temperatures(carbon_levels_without_holidays))
morning = ~np.isin(carbon_levels_without_holidays.index.time, (pd.date_range("8:00", "9:30", freq = "1min").time)) #This will be used to denote the time periods in early morning
carbon_levels_test = carbon_levels_without_holidays[~morning]
#print(carbon_levels_test)
new_name = input("Enter a sensor name to check: ")
if (carbon_levels_test[new_name].quantile(0.65) < 68):
    print("Bad system.")
    print(carbon_levels_test[new_name].quantile(0.65))
else:
    print("Good system with a temp of " + str(carbon_levels_test[new_name].quantile(0.65)) + " degrees Fahrenheit.")
print("AC Need")
print(find_rooms_needing_AC(carbon_levels_without_holidays))
print(carbon_levels_without_holidays)
print(flag_sensors(carbon_levels_test))

high_list, medium_list, light_list = sort_sensors(carbon_levels_without_holidays)

print("High sensors are " + str(high_list))
print(medium_list)

print("Broken sensors: ")
print(broken_sensors(carbon_levels_without_holidays))


while not terminate_loop:
    column_name = input("Enter a sensor name to check if its system is all-set: ")
    filter_level = input("Checking system for T or CO2: ").lower()
    if filter_level == "t":
        if column_name in carbon_levels_without_holidays.columns:
            if ((abs(carbon_levels_without_holidays[column_name].quantile(0.15)-targ1) < 2) and (abs(carbon_levels_without_holidays[column_name].quantile(0.85)- targ2) < 2)):
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
    elif filter_level == "co2":
        if column_name in carbon_levels_without_holidays.columns:
            if ((carbon_levels_without_holidays[column_name].quantile(0.45)) < 800):
                print("Good system")
                print("\nThis system runs from " + str(carbon_levels_without_holidays[column_name].quantile(0.45)) + " ppm to " + str(carbon_levels_without_holidays[column_name].quantile(0.85)) + " ppm.")
                while v != "yes":
                    v = input("Check again? ").lower()
                    if v == "no":
                        terminate_loop = True
                        break
                    elif v != "yes":
                        print("Invalid, try again: ")

            else:
                print("Bad system")
                print("\nThis system runs from " + str(carbon_levels_without_holidays[column_name].quantile(0.45)) + " ppm to " + str(carbon_levels_without_holidays[column_name].quantile(0.85)) + " ppm.")
                while v != "yes":
                    v = input("Check again? ").lower()
                    if v == "no":
                        terminate_loop = True
                        break 
                    elif v != "yes":
                        print("Invalid, try again: ")

'''




        





                


