import boto3
import requests
from datetime import datetime
import numpy as np
import s3fs
import json
import pandas as pd
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
import sys
from io import StringIO
from time import sleep

# Get bucket argument from stack file
job_args = getResolvedOptions(sys.argv, ["my_bucket"])

# fetch API key
def get_secret():

    secret_name = "finalproject/daniel/ticketmaster"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:

        raise e

    secret = get_secret_value_response['SecretString']

    return secret

secret_string = get_secret()
parsed_json = json.loads(secret_string)
api_key = parsed_json["TICKETMASTER_API_KEY"]

# Inititalize boto3 client
s3 = boto3.client("s3")

# Base URL for API requests
base_url = 'https://app.ticketmaster.com/discovery/v2/events.json?'

# Parameters for the request
country_code = 'US'
page_size = 9  # Number of events per page
current_page = 0  # Starting page
total_pages = None  # Will be determined after the first request

# Specify column name dictionary to be turned into pandas dataframe later
columns = {"Name": [], 
            "Date": [], 
            "Time": [], 
            "Category": [], 
            "SubCategory": [],
            "Max": [], 
            "Min": [], 
            "SalesStart": [], 
            "SalesEnd": [],
            "Status": [], 
            "City": [],
            "State": [], 
            "Latitude": [],
            "Longitude": [],
            "Description": [],
            "Currency": [],
            "URL": [],
            "ID": [],
            "Venues": []}

# Loop through pages of results
while total_pages is None or current_page < total_pages:
    # Construct the URL with current page and page size
    url = f'{base_url}countryCode={country_code}&apikey={api_key}&size={page_size}&page={current_page}'
    sleep(0.2)

    
    # Make GET request to the API
    response = requests.get(url)
    
    # Handle the response
    if response.status_code == 200:
        data = response.json()
        events = data['_embedded']['events']

        # Process the events and load to dictionary
        for event in events:
            name = event['name']
            columns['Name'].append(name)

            date = datetime.strptime(event['dates']['start'].get('localDate', None), '%Y-%m-%d').date()
            columns['Date'].append(date)
            
            time = event['dates']['start'].get('localTime', None)
            time = datetime.strptime(time, '%H:%M:%S').time() if time is not None else None
            columns["Time"].append(time)

            category = event.get('classifications', [{}])[0].get('genre', {}).get('name', 'N/A')
            columns['Category'].append(category)

            subcategory = event.get('classifications', [{}])[0].get('subGenre', {}).get('name', 'N/A')
            columns['SubCategory'].append(subcategory)
            
            max = event.get('priceRanges', [{}])[0].get('max', 'N/A')
            columns['Max'].append(max)

            min = event.get('priceRanges', [{}])[0].get('min', 'N/A')
            columns['Min'].append(min)

            currency = event.get('priceRanges', [{}])[0].get('currency', 'N/A')
            columns['Currency'].append(currency)

            url = event.get('url', 'N/A')
            columns['URL'].append(url)

            id = event.get('id', 'N/A')
            columns['ID'].append(id)

            venues = event.get("_embedded", {}).get('venues', [{}])[0].get('name', 'N/A')
            columns["Venues"].append(venues)
            
            start_date_time = event.get('sales', {}).get('public', {}).get('startDateTime', None)
            start_date_time = datetime.strptime(start_date_time, "%Y-%m-%dT%H:%M:%SZ") if start_date_time is not None else None
            start_date_time = start_date_time if start_date_time is not None else None
            columns['SalesStart'].append(start_date_time)

            end_date_time = event.get('sales', {}).get('public', {}).get('endDateTime', None)
            end_date_time = datetime.strptime(end_date_time, "%Y-%m-%dT%H:%M:%SZ") if end_date_time is not None else None
            columns['SalesEnd'].append(end_date_time)

            status = event['dates']['status']['code']
            columns['Status'].append(status)

            city = event.get('_embedded', {}).get('venues', [{}])[0].get('city', {}).get('name', 'N/A')
            columns['City'].append(city)

            state = event.get('_embedded', {}).get('venues', [{}])[0].get('state', {}).get('name', 'N/A')
            columns['State'].append(state)

            latitude = event.get('_embedded', {}).get('venues', [{}])[0].get('location', {}).get('latitude', 'N/A')
            columns['Latitude'].append(latitude)
            
            longitude = event.get('_embedded', {}).get('venues', [{}])[0].get('location', {}).get('longitude', 'N/A')
            columns['Longitude'].append(longitude)
            
            description = event.get('info', 'N/A')
            columns['Description'].append(description)
        
        # Update total_pages after the first request
        if total_pages is None:
            total_pages = data['page']['totalPages']
        
        # Increment the current page to fetch next page in the next iteration
        current_page += 1
    else:
        error_code = response.status_code
        text = response.text
        break

# Convert dictionary to pandas dataframe
my_data_frame = pd.DataFrame(columns)
my_info = {"error_code": error_code,
           "error_text": text,
            "current_page": current_page,
            "DataFrame_Length": len(my_data_frame)
}

json_string = json.dumps(my_info).encode('utf-8')


# Write to S3
my_data_frame.to_csv(f"s3://{job_args['my_bucket']}/ticketmaster.csv")
s3.put_object(Bucket=job_args['my_bucket'], Key="info.json", Body=json_string, ContentType='application/json')