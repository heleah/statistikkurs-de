
#!/usr/bin/env python
# coding: utf-8

# # Import libs and methods
import pandas as pd
import os
import time
import requests
from sql_functions import unix_to_timestamp
import numpy as np
from sql_functions import encrypt
from sql_functions import upload_dataframe
from dotenv import load_dotenv

def run_full_dl_master():
    table_name = input("Wie soll deine NEUE Tabelle heißen?")
    load_dotenv()

    # # Download

    # Define the maximum amount of pages exist in Subscriptions (just to make sure that we can download everything in the future)
    url = "https://app.webinargeek.com/api/v2/subscriptions"
    headers = {"Api-Token": os.getenv('webinargeek_api_key'),"Content-Type": "application/json"}
    querystring = {"per_page": 1000} 

    # unpack json file
    response = requests.get(url, headers=headers, params=querystring) 
    subscription_json = response.json()

    # get number of pages
    pages = subscription_json.get("pages").get("total_pages")
    pages = list(range(1, pages + 1))

    # Download all data
    subs_api = pd.DataFrame()

    for page in pages:
        # Get Subscriptions from API
        url = "https://app.webinargeek.com/api/v2/subscriptions"
        headers = {"Api-Token": os.getenv('webinargeek_api_key'), "Content-Type": "application/json"}
        querystring = {"per_page": 1000, "page": page}

        # unpack json file
        response = requests.get(url, headers=headers, params=querystring) 
        subscription_json = response.json()

        #change relevant data to data_frame
        temporary_df = subscription_json.get("subscriptions")
        temporary_df = pd.DataFrame(temporary_df)

        #concat the data_frames
        subs_api = pd.concat([temporary_df, subs_api])
        
    subs_api_ids = subs_api['id'].to_list()

    # Download new Data with help of Sub IDs 
    new_data = pd.DataFrame()

    for id in subs_api_ids:
        # Get Subscriptions from API
        url = f"https://app.webinargeek.com/api/v2/subscriptions/{id}"
        headers = {"Api-Token": os.getenv('webinargeek_api_key'),"Content-Type":"application/json"}

        # unpack json file
        response = requests.get(url, headers=headers) 
        
        #One request every 800 ms. Results in 4500 requests in one hour or 75 in a minute.
        time.sleep(800/1000) 
        
        subscription_json = response.json()

        #change relevant data to data_frame
        temporary_df = pd.json_normalize(subscription_json)

        #concat the data_frames
        new_data = pd.concat([temporary_df, new_data])

    new_data.reset_index(inplace=True, drop=True)
    print("Download fertig, Cleaning startet")

    # # Cleaning
    # make sure empty strings are "None"
    new_data.apply(lambda x: x.replace('', None, inplace=True))

    # change column names to use "_" instead of "."
    new_data.rename(columns=lambda x: x.replace('.', '_'), inplace=True)

    # create timestamps and delete old columns
    date_columns = ["created_at", "email_verified_at", "watch_end", "watch_start", "broadcast_date", "broadcast_ended_at", "broadcast_started_at"]

    for column in date_columns:
        unix_to_timestamp(new_data, column)
        new_data.drop(columns=column, inplace=True)
        new_data[f"{column}_timestamp"].replace({np.NaN:None}, inplace=True)

    # get rid of whitespaces
    new_data = new_data.apply(lambda x: x.str.strip() if type(x) == "string" else x)

    # encrypt emails
    encrypt(new_data,"email")

    # list all unwanted columns
    new_data_columns = []
    for column in new_data.columns:
        new_data_columns.append(column)

    mastertable_columns = [""] # enter column names to keep
    drop_list = []

    for i in new_data_columns:
        if i not in mastertable_columns:
            drop_list.append(i)

    # drop unnecessary columns
    new_data.drop(columns=drop_list, inplace=True)

    # correct values of df['broadcast_duration'] of webinar 'Teil 1: Hypothesentest (1. – 7. Juni)' to 7260
    new_data.loc[new_data['webinar_title'] == 'Teil 1: Hypothesentest (1. – 7. Juni)', 'broadcast_duration'] = 7260

    # create column with watch_duration / broadcast_duration as watch_percentage
    new_data['watch_percentage'] = new_data['watch_duration']/ new_data['broadcast_duration']
    print("Cleaning fertig, Upload startet, bitte warten")

    # # Upload
    upload_dataframe(new_data,"public",table_name)
    print("Upload beendet, Programm kann geschlossen werden")
