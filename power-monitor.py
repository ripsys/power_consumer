#!/usr/bin/env python
from MCP3008 import MCP3008
from math import sqrt
from influxdb import InfluxDBClient
import datetime
import time
import csv
import logging
from scipy.signal import savgol_filter
import array
import holidays
from socket import socket, AF_INET, SOCK_DGRAM


#Config 

adc = MCP3008()
raw_voltage_data = []
Umax = float(0)
ct1_A = float(0)
ct2_A = float(0)
voltage_data = []
acc_factor_vac = 0.535
peak_count = 0
peak = 0
ct1_peak = 0
ct2_peak = 0
V_peak = 0
peak_value_raw = 0
ct1_raw_data = []
ct1_normalized_data = []
ct1_raw_accuracy_data = 290
ct2_raw_data = []
ct2_normalized_data = []
ct2_raw_accuracy_data = 290
volts_raw_data = []
volts_normalized_data = []
peak_array_V = array.array ('i', (0 for i in range(0,200)))
peak_array_ct1 = array.array ('i', (0 for i in range(0,200)))
peak_array_ct2 = array.array ('i', (0 for i in range(0,200)))
peak_array = array.array ('i', (0 for i in range(0,2000)))

# Define holidays country as Canada (BC is default Province)
ca_holidays = holidays.Canada()
time_elapsed = (0)
rate = 0.12



def get_raw_data(ch_num, periods_number):
    data = []
    now = datetime.utcnow()
    counter = 0
    while len(data) <= periods_number:
        data.append(adc.read(ch_num))
    return data

def get_raw_power_data (ch_num):
    data = 0
    data = adc.read(ch_num)
    return data

def normalize_A_wave (data):
    new_wave = []
    record_length = len(data)-1
    
    for i in range (0,record_length):
        
        if i > 0 & i < record_length:
            data[i] = round((data[i-1] / 4 + data[i] / 2 + data[i+1] / 4))
            
        new_wave.append(round(data[i]))
    return new_wave


def find_peak_V (raw_data, old_peak_value, old_count):
    peak_value_raw = old_peak_value
    next_data = 0
    prev_data = 0
    current_data = 0
    i = 0
    count = old_count
    record_length = len(raw_data)-10
    while (i < record_length):
        
        if i > 0 & i < record_length:
            next_data = raw_data[i+1]
            prev_data = raw_data[i-1]
            current_data = raw_data[i]
        
        if (current_data > prev_data) & (current_data > next_data) & (current_data > 0):
            if current_data > raw_data[i+10]:
                peak_value_raw += current_data
                count += 1
                peak_array[i] = 1
            
        i += 1
    
    try :
        peak_value = peak_value_raw / count
    except ZeroDivisionError:
        peak_value = 0
    return peak_value, count, peak_value_raw

def find_peaks_A (raw_data):
    peak_value_raw = 0
    next_data = 0
    prev_data = 0
    current_data = 0
    i = 0
    j = 0
    peak_array_A = array.array ('i', (0 for i in range(0,200)))
    count = 0
    record_length = len(raw_data)-1
    
    while (i < record_length):
        
        if i > 0 & i < record_length:
            next_data = raw_data[i+1]
            prev_data = raw_data[i-1]
            current_data = raw_data[i]
           
        if (current_data > prev_data) & (current_data > next_data) & (current_data > 0):
            peak_value_raw += current_data
            count += 1
            peak_array_A[j] = i
            j += 1            
        i += 1
        
    try :
        peak_value = peak_value_raw / count
    except ZeroDivisionError:
        peak_value = 0
        
    return peak_array_A, peak_value


def write_datafile(filename, data1, data2):
    now = datetime.utcnow ()
    with open(filename,'w') as f:
        headers = ["Sample#", "Raw Data", "Harmonized Data"]
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(0, len(data1)-1):
            writer.writerow([i,round(data1[i],2), round(data2[i],2)])

def get_power_data (number_of_samples):
    ch0_data = []
    ch1_data = []
    ch5_data = []
    
    for k in range (0, number_of_samples):
        ch0_data.append(get_raw_power_data(0))
        ch1_data.append(get_raw_power_data(1))
        ch5_data.append(get_raw_power_data(5))
        
    return ch0_data, ch1_data, ch5_data


while True:
    
    for j in range(0, 200) :
        peak_array_V[j] = 0
        peak_array_ct1[j] = 0
        peak_array_ct2[j] = 0
        
    '''for j in range(0, 2000):
        peak_array[j] = 0'''
        
    kilowatts = float(0)
    
    # start timer to use in kWh calculations
    start_time = time.time()
    
    ct1_raw_data, ct2_raw_data, volts_raw_data = get_power_data(2000)
        
    voltage_data = savgol_filter(volts_raw_data, 10, 3)
    ct1_normalized_data = savgol_filter(ct1_raw_data, 10, 3)
    ct2_normalized_data = savgol_filter(ct2_raw_data, 10, 3)
        
    peak, peak_count, peak_value_raw = find_peak_V(voltage_data, peak_value_raw, peak_count)
    peak_array_ct1, ct1_peak = find_peaks_A (ct1_normalized_data)
    peak_array_ct2, ct2_peak = find_peaks_A (ct2_normalized_data)
    peak_array_V, V_peak = find_peaks_A (voltage_data)
    
    if peak_count > 1200:
        peak_value_raw = 0
        peak_count = 0
    
    half_peak = peak / 2
    Umax = half_peak    
    U = Umax / sqrt(2) * acc_factor_vac
    
    ct1_A = ((ct1_peak - ct1_raw_accuracy_data)/ 2) / sqrt(2)
    
    ct2_A = ((ct2_peak - ct2_raw_accuracy_data) / 2) / sqrt(2)
    if ct1_A < 0.2 :
        ct1_A = 0.0
    if ct2_A < 0.2 :
        ct2_A = 0.0
    
    # Calculate total AMPS from all sensors and convert to kilowatts
    kilowatts = round((ct1_A + ct2_A) * U/1000,2)
    
    # convert kilowatts to kilowatts / hour (kWh)
    kwh = round((kilowatts * time_elapsed)/3600,8)
    
    # Calculate estimated cost / hour assuming current usage is maintained for an hour
    cph = round((kilowatts * rate),2)

    # iso = datetime.datetime.now()
    iso = datetime.datetime.utcnow().isoformat() + 'Z'

    
    # write all ampere readings to database
    json_amps = [
        {
            "measurement": "current",
            "time": iso,
            "fields": {
                "CT_1": ct1_A,
                "CT_2": ct2_A,
            }
        }
    ]
    
    # Change the client IP address and user/password to match your instance of influxdb
    # Note that I have no user or password, place them in the quotes '' after port number
    # retries=0 means infinate attempts.
    client = InfluxDBClient('10.0.0.168', 8086, '', '', 'ampread', timeout=60,retries=3)
    try:
        client.create_database('ampread')
        client.write_points(json_amps)
    except ConnectionError:
        print('influxdb server not responding')
        continue
    
     # write voltage, rate, kW, kWh, and cost/hr to influx
    json_misc = [
        {
            "measurement": "voltage",
            "time": iso,
            "fields": {
                "voltage": U,
                "rate": rate,
                "cph": cph,
                "kwh": kwh,
                "kilowatts": kilowatts,
            }
        }
    ]

    client = InfluxDBClient('10.0.0.168', 8086, '', '', 'ampread', timeout=60,retries=3)
    try:
        client.create_database('ampread')
        client.write_points(json_misc)
    except ConnectionError:
        print('influxdb server not responding')
        continue
    
   
    except KeyboardInterrupt:
        print('You cancelled the operation.')
        sys.exit()
    
    
    print ("U = ", round(U,2), "V" , "CT1 =", round(ct1_A,2), "A", "CT2 =", round(ct2_A,2), "A"  )
    print ("Peaks count is ", peak_count)
    print ("Current power consumption ", kilowatts, " kW")
    print()
    
    time_elapsed = time.time() - start_time
     
            
    


