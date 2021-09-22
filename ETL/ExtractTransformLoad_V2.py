#!/usr/bin/env python

import glob                         # this module helps in selecting files kinda like grep
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime


import requests
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
down = requests.get(url, allow_redirects=True)
open('source.zip', 'wb').write(down.content)

import shutil
shutil.unpack_archive('source.zip', 'source extracted') 

tmpfile    = "temp.tmp"               # file used to store all extracted data
logfile    = "logfile.txt"            # all event logs will be stored in this file
targetfile = "transformed_data.csv"   # file where transformed data is stored


# CSV Extract Function
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# JSON Extract Function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe


# XML Extract Function
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) # creating an empty dataframe with columns
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe


# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("*source extracted/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True) # we can append a dataframe to an existing dataframe too
        
    #process all json files
    for jsonfile in glob.glob("source extracted/*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("source extracted/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data


# Transform
def transform(data):
        #Convert height which is in inches to millimeter
        #Convert the datatype of the column into float
        #data.height = data.height.astype(float)
        #Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
        data['height'] = round(data.height * 0.0254,2)
        
        #Convert weight which is in pounds to kilograms
        #Convert the datatype of the column into float
        #data.weight = data.weight.astype(float)
        #Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
        data['weight'] = round(data.weight * 0.45359237,2)
        return data


# Loading
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)  #loads the transformed data into a csv file finally using the .to_csv to convert the df to csv


# Logging
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format) # .strftime(format) ; the strftime formats the datetime object in the format specified
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")
extracted_data

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")
transformed_data 

log("Load phase Started")
load(targetfile,transformed_data)
log("Load phase Ended")

log("ETL Job Ended")


# Exercise


import requests
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
file = requests.get(url, allow_redirects=True)
open('datasource.zip', 'wb').write(file.content)

import shutil
shutil.unpack_archive('datasource.zip', 'datasource_extracted')


tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

def extractcsv(file):
    df = pd.read_csv(file)
    return df

def extractjson(file):
    df = pd.read_json(file, lines=True)
    return df

def extractxml(file):
    df = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file)  #first we parse the xml file
    root = tree.getroot()  #then we get the root of the tree
    for car in root:       #then we go through every record in the tree and append it to the dataframe 
        model = car.find("car_model").text
        year = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = df.append({"car_model":model, "year_of_manufacture":year, "price":price, "fuel":fuel}, ignore_index=True)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("datasource_extracted/*.csv"):
        extracted_data = extracted_data.append(extractcsv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("datasource_extracted/*.json"):
        extracted_data = extracted_data.append(extractjson(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("datasource_extracted/*.xml"):
        extracted_data = extracted_data.append(extractxml(xmlfile), ignore_index=True)
        
    return extracted_data

def transform(data):
    data['price'] = round(data.price, 2)
    return data
    

def load(data_to_load, file_to_load_to):
    data_to_load.to_csv(file_to_load_to)

def log(message):
    tformat = '%H: %M: %S -%h-%d-%Y'
    now = datetime.now()
    timestamp = now.strftime(tformat)
    with open("dealership_log.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


log("ETL Process started")

log("Extract process started")
data = extract()
log("Extract process finished")

log("Transform process started")
transdata = transform(data)
log("Transform process finished")

log("Load process started")
load(transdata, targetfile)
log("Load process finished")

log("ETL job ended")
