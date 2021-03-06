#! /usr/bin/python3
#run python janeapp-scrap 

import requests
import boto3
import datetime
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")
h_dir = '/home/ubuntu/getTCM/'
#h_dir = 'C:/_Files/Octoparse/Scraper/'

# Function to scrape Jane websites
def scrapeBooking(url, clinicNo):
    base, params = url.split("#")
    param_values = params.split("/")
    
    if len(param_values) > 4:
        index1 = base.find('//')
        index2 = base.find('/', index1 + 2)
        host = base[0:index2+1]
        
        if param_values[1]=="staff_member":
            staff_member = param_values[2];
        if param_values[3]=="treatment":
            treatment = param_values[4]
            
        r1 = requests.get(base, verify=False)
        c = r1.cookies
        
        now = datetime.datetime.now()
        startDate = now.strftime("%Y-%m-%d")
        #startDate = ("2019-03-18")
        
        openings_all = pd.DataFrame()
        shifts_all = pd.DataFrame()     
    
        apiurl = host + "api/v2/openings?location_id=" + str(clinicNo) + "&staff_member_id=" + staff_member + "&treatment_id=" + treatment + "&date=" + startDate + "&num_days=7"
        
        try:
            r2 = requests.get(apiurl, cookies = c, verify=False)        
            data = r2.json()
            
            full_name = data[0]["full_name"]
            openings = data[0]["openings"]
            shifts = data[0]["shifts"]
            
            # Building dataframe of openings
            openings_df = pd.DataFrame.from_records(openings)
            if len(openings) > 0:
                openings_df['Clinician'] = full_name
                openings_df['URL']=url
                openings_df['Today'] = startDate
                openings_df['DayOfWeek'] = pd.to_datetime(openings_df['start_at']).dt.day_name()
                openings_df['Date'] = pd.to_datetime(openings_df['start_at']).dt.strftime('%Y-%m-%d')
                openings_df['Start'] = pd.to_datetime(openings_df['start_at']).dt.strftime('%H:%M')
                openings_df['End'] = pd.to_datetime(openings_df['end_at']).dt.strftime('%H:%M')
                openings_df['Duration'] = (pd.to_datetime(openings_df['end_at']) - pd.to_datetime(openings_df['start_at']))
                openings_df['Duration'] = openings_df['Duration'].dt.total_seconds().div(60).astype(int)
                openings_df['DaysAhead'] = (pd.to_datetime(openings_df['end_at']) - pd.to_datetime(startDate+'T00:00:00'))
                openings_df['DaysAhead'] = openings_df['DaysAhead'].dt.total_seconds().div(60*60*24).astype(int)  
                openings_df['HrsAhead'] = (pd.to_datetime(openings_df['end_at']) - pd.to_datetime(startDate+'T00:00:00'))
                openings_df['HrsAhead'] = openings_df['HrsAhead'].dt.total_seconds().div(60*60).astype(int)
                openings_df = openings_df.loc[openings_df['HrsAhead'] >= 0]          
                openings_df = openings_df.drop(['duration', 'end_at', 'id', 'location_id', 'parent_appointment_id',
                                                'persisted','room_id', 'staff_member_id','start_at','status', 'HrsAhead'], axis=1)
                openings_all = openings_all.append(openings_df, ignore_index=True)
            
            # Building dataframe of working hours
            shifts_df = pd.DataFrame.from_records(shifts)
            if len(shifts) > 0:
                shifts_df['Clinician'] = full_name
                shifts_df['URL'] = url
                shifts_df['Today'] = startDate
                shifts_df['DayOfWeek'] = pd.to_datetime(shifts_df['start_at']).dt.day_name()
                shifts_df['Date'] =  pd.to_datetime(shifts_df['start_at']).dt.strftime('%Y-%m-%d')
                shifts_df['Start'] = pd.to_datetime(shifts_df['start_at']).dt.strftime('%H:%M')
                shifts_df['End'] = pd.to_datetime(shifts_df['end_at']).dt.strftime('%H:%M')
                shifts_df['Duration'] = (pd.to_datetime(shifts_df['end_at']) - pd.to_datetime(shifts_df['start_at']))
                shifts_df['Duration'] = shifts_df['Duration'].dt.total_seconds().div(60).astype(int)
                shifts_df['DaysAhead'] = (pd.to_datetime(shifts_df['end_at']) - pd.to_datetime(startDate+'T00:00:00'))
                shifts_df['DaysAhead'] = shifts_df['DaysAhead'].dt.total_seconds().div(60*60*24).astype(int)
                shifts_df['HrsAhead'] = (pd.to_datetime(shifts_df['end_at']) - pd.to_datetime(startDate+'T00:00:00'))
                shifts_df['HrsAhead'] = shifts_df['HrsAhead'].dt.total_seconds().div(60*60).astype(int)
                shifts_df = shifts_df.loc[shifts_df['HrsAhead'] >= 0]
                shifts_df = shifts_df.drop(['call_to_book', 'end_at', 'start_at', 'HrsAhead'], axis=1)
                shifts_all = shifts_all.append(shifts_df, ignore_index=True)
        except:
            pass
                    
        return openings_all, shifts_all
    
    return None


# Reading in list of jane urls
try:
    clinics = pd.read_csv(h_dir + 'jane_url.csv')

    # Initializing data frames
    errors = []
    openings_df = pd.DataFrame()
    shifts_df = pd.DataFrame()
    
    # Looping through all urls
    for x, row in clinics.iterrows():
        
        try:
            openings, shifts = scrapeBooking(row['Url'], row['Location'])
            
            if openings.shape[1] > 0:
                openings_df = openings_df.append(openings, ignore_index=True)
            
            if shifts.shape[1] > 0:
                shifts_df = shifts_df.append(shifts, ignore_index=True)
            
            print("Completed " + str(x) + " of " + str(len(clinics.Url)) + " urls")
                
        except:
            print("Error on url " + row['Url'])
            errors.append(row['Url'])
    
    
    # Saving csvs
    try:
        now = datetime.datetime.now()
        saveDate = now.strftime("%Y%m%d")
        #saveDate = 'SAMPLE'
        openings_df.to_csv(h_dir + 'openings' + saveDate + '.csv', index = None)
        shifts_df.to_csv(h_dir + 'shifts' + saveDate + '.csv', index = None)
        
        with open(h_dir + 'error' + saveDate + '.txt', 'w') as f:
            for item in errors:
                f.write("%s\n" % item)

        # Upload file to S3 bucket
        s3 = boto3.resource('s3')
        
        try:
            s3_name = 'openings' + saveDate + '.csv'
            f_name = h_dir + s3_name
            s3.Bucket('tcmbooking').upload_file(f_name, s3_name)
            os.remove(h_dir + 'openings' + saveDate + '.csv')
        except:
            print('Could not save openings to s3 bucket')
         
        try:
            s3_name = 'shifts' + saveDate + '.csv'    
            f_name = h_dir + s3_name    
            s3.Bucket('tcmbooking').upload_file(f_name, s3_name)
            os.remove(h_dir + 'shifts' + saveDate + '.csv')
        except:
            print('Could not save shifts to s3 bucket')
                 
        try:
            s3_name = 'error' + saveDate + '.txt'
            f_name = h_dir + s3_name
            s3.Bucket('tcmbooking').upload_file(f_name, s3_name)
            os.remove(h_dir + 'error' + saveDate + '.txt')
        except:
            print('Could not save errors to s3 bucket')
        
    except:
        print('Error writing data frames to file')
        

except:
    print('could not read in urls')


