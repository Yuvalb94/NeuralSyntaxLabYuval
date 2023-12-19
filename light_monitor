import datetime
import sys
import os
import time
import yaml 
from zoneinfo import ZoneInfo
from argparse import ArgumentParser

import serial
import serial.tools.list_ports
import pandas as pd

SERIAL_PORT_DATA_RATE = 9600

POSSIBLE_DEVICE_PATHS = [
    "/dev/ttyACM0", 
    "/dev/ttyACM1", 
    # Debugging on Yuval's mac mini - 
    "/dev/cu.usbmodem2401",
    "/dev/cu.usbmodem2301",
    # Debugging on a PC - 
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5" 
] 



LAST_LIGHT_SWITCH_STATE = None

# These are the names of the CSV columns.
# NOTE - The last column must be dateTime, since we're counting the rest of the fields to verify
# the data vailidity!
CSV_FIELD_NAMES = ['dateTime','lights_on_time','lights_off_time']

FILE_WRITE_DELAY_MINS = 3 # How many minutes we wait between each file dump
TIMEZONE_NAME = "Asia/Jerusalem"


def get_serial_device():
    """
    Get the device object for our serial port.

    TODO - Find a smarter way to find the device in case it has a different name 
    (e.g. - can be /dev/ttyACM0, /dev/ttyACM1 and so on)
    """   
    for path in POSSIBLE_DEVICE_PATHS:
        try:
            ser = serial.Serial(path, SERIAL_PORT_DATA_RATE, timeout=1)
            print(f"\tSuccessfully opened serial port {path}")
            return ser
        except serial.SerialException:
            pass

    raise Exception(f"No valid serial port could be found! Tried the following - {POSSIBLE_DEVICE_PATHS}")

def FindArduinoPort(SearchFor):
    """
    this function search for the port Arduino is connected to based upon a search word such as the device name

    ###ARGS###
    SearchFor - a string according to the function will search for the port.
    """     
    ports = list(serial.tools.list_ports.comports())
    # Iterate through each port and check if it's an Arduino board
    for port in ports:
        if SearchFor in port.description:
            print(f"Arduino board found on {port.device}")
            SerialPort=port.device
    return SerialPort  

def parse_arduino_data(arduino_raw_data):
    """
    this function accepts raw data from the serial port arduino is connected to and edit it such that we will get numbers, with no space between lines.

    ###ARGS###
    arduino_raw_data - a one line with semicolon as delimeter between values for example : 50;300;14.6
    """
    try:
        raw_data = str(arduino_raw_data,'utf-8')

        data_packet = raw_data
        data_packet = data_packet.strip('\r\n')
        data_packet = data_packet.split(";")
        data_packet = [float(x) for x in data_packet]
    except Exception as err:
        print(f"Failed parsing a row - {raw_data}. Error - {err}")
        return None
    return data_packet



def data_aggregation(list_of_data_points):
    """
    Receive a list of data points, where each datapoint is a dictionary, e.g. - 
        {"humidity": 6, "temp": 1.3}
    
    and return an aggregated dictionary, with the following values - 
    {"min_humidity": ..., "max_humidity": ... , .. , ...} and so on for all values of the original dictionary
    """
    df = pd.DataFrame(list_of_data_points)

    aggregated_data = {}
    try:
        for field in CSV_FIELD_NAMES:
            if field != "dateTime":
                aggregated_data[f"{field}_min"] = str(df[field].min())
                aggregated_data[f"{field}_max"] = str(df[field].max())
                aggregated_data[f"{field}_median"] = str(df[field].median())
    except Exception as err:
        print(f"Failed aggregating data - ")
        print(list_of_data_points)
        print("#####")
        print("ERROR -")
        print(err)

    current_time = datetime.datetime.now()
    aggregated_data["dateTime"] = current_time

    return aggregated_data
    

def get_arduino_data(serial_device):
    """
    Read & parse sensor data from the arduino device (via the serial port).
    We return a dict with the parsed data from the sensors.
    """
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y_%m_%d_%H_%M_%S.%f")

    try:
        arduino_raw_data = serial_device.readline()
    except Exception as err:
        print(f"Failed reading data from Arduino! {err}")
        return None

    data_packet = parse_arduino_data(arduino_raw_data)

    # Verify that the data was read properly. 
    # If our data dict has less than the expected CSV field name count (minus 1 for the datetime field
    # which we manually add), then there's been an error with reading the data 
    if (data_packet is None) or (len(data_packet) != (len(CSV_FIELD_NAMES) - 1)):
        print(f"Failed parsing data. Ignoring this record! (raw data was - {arduino_raw_data})")
        return None

    data_packet.append(formatted_time)

    tuples = [(key, value) for i, (key, value) in enumerate(zip(CSV_FIELD_NAMES, data_packet))]

    return dict(tuples)
    


if __name__ == "__main__":
    print("Hello! This is the Arduino controller script!\n")

   
    ## Part 1 - Connect to the serial device
    try:
        serial_device = get_serial_device()
        print("\tSuccessfully connected to Serial device")
    except Exception as err:
        print(f"Failed connecting to the Serial device - `{err}`")
        sys.exit(1)
    
    ## Part 4 - Initialize a Weizmann location object for the Astral package
    try:
        wis_location_info = get_weizmann_location_object()
        print("\tSuccessfully initialized the location object for WIS")
    except Exception as err:
        print(f"Failed initializing the location object - `{err}`")
        sys.exit(1)
    
    print("\n")

    ## Part 5 - This is the main part of the code, which runs in a loop and reads data from
    ## sensors, and controls the light switch.
    
    data_from_last_hour = [] # This array will handle the hourly data, and will be reset once an hour is over

    hour_loop_start_time = datetime.datetime.now()
    while True: 
        while serial_device.in_waiting == 0: 
            pass 
        
        # Part 5.1 - Handle the lights! Turn the lights on/off, and update our state variable based on the action
        LAST_LIGHT_SWITCH_STATE = handle_lights(serial_device, config_data, wis_location_info, slack_client)

        # Part 5.2 - Read & aggregate data from the sensor
        current_time = datetime.datetime.now()
        print(f"Current UTC time is {current_time}\n")

        
        data_from_last_minute = []

        minute_loop_start_time = datetime.datetime.now()

        # This loop should run for 1 minute, with a 1 second rest in between.
        # Every second we get new data from the arduino, and store in an array.
        if config_data["dataReadingAndSaving"]:
            while True:
                data = get_arduino_data(serial_device)

                if data is None: # There was an error, moving on and ignoring this specific read
                    continue

                data_from_last_minute.append(data)

                if (datetime.datetime.now() - minute_loop_start_time).seconds >= 60:
                    print("\t\tFinished collecting data for 1 minute")
                    break
                time.sleep(1)
            
            # After we recorded data for 1 minute, we aggregate it and store in the 1 hour array.
            aggregated_data = data_aggregation(data_from_last_minute)
            print("\t\tSuccessfully aggregated data")
            data_from_last_hour.append(aggregated_data)

            minutes_since_start = (datetime.datetime.now() - hour_loop_start_time).seconds / 60 
            if minutes_since_start >= FILE_WRITE_DELAY_MINS:
                print(f"\t{FILE_WRITE_DELAY_MINS} minutes have passed! Writing data to disk")

                curr_time = datetime.datetime.now().strftime("%Y%m%d_%H_%M_%S")
                
                days_offset = config_data.get("days_offset", 0)
                stable_date=config_data.get("stable_date", 0)
                sunrise = config_data.get("sunrise",0)
                sunset =config_data.get("sunset",0)
                
                if (sunrise is not None) & (sunset is not None):
                    file_name=file_name_set_manuall=f"cage_{str(config_data['cage_id'])}_{config_data['bird_name']}_{curr_time}_manually_set.csv"
                elif stable_date is not None:
                    file_name=file_name_stable_date=f"cage_{str(config_data['cage_id'])}_{config_data['bird_name']}_{curr_time}_stable_date_{str(stable_date)}.csv"
                elif type(days_offset) == type(1):
                    file_name=file_name_offset = f"cage_{str(config_data['cage_id'])}_{config_data['bird_name']}_{curr_time}_Days_offset_{str(days_offset)}.csv"
                
                
                output_path = os.path.join(config_data["dataOutputBasePath"], file_name)
                
                hourly_data_df = pd.DataFrame(data_from_last_hour)
                hourly_data_df.to_csv(output_path)

                print(f"\tSuccessfully wrote hourly data to {output_path}")
                
                # Reset the hour loop start time, the hourly data array, and then continue the loop.
                hour_loop_start_time = datetime.datetime.now()
                data_from_last_hour = [] 
        else:
            print('User chose not to print and save data')
            time.sleep(3600)
                
            
        
    


 ## Questions for Yarden - 
 # 1. Is the config file idea fine? or is it too complex?
 # 2. Read device name from Config file?
 # 3. Is the time offset implemented correctly?
    
## TODO AFTER MEETING WITH IDO - 
# 1. docs for using slack api
