# A utility to interact with the api from eloverblik.dk
# By Morten Helmstedt, https://helmstedt.dk
# API documentation:
# https://api.eloverblik.dk/CustomerApi/swagger/index.html
# https://www.niras.dk/media/4vbbvkig/eloverblik-adgang-til-egne-data-via-api-kald-forkortet-1.pdf
import argparse
import csv
from datetime import date, datetime, timedelta
import os
from os.path import exists
import pickle
import requests
import sys
import time
from zoneinfo import ZoneInfo

# Set token filename
token_filename = 'eloverblik.token'
data_access_token_filename = 'eloverblik_data_access.token'

# Number of API retries (API often returns 503 errors)
api_retries = 10

# API base url
base_url = 'https://api.eloverblik.dk/CustomerApi/api/'

# Get today's date
today = date.today()

# Prepare session for requests
session = requests.Session()

# Set session headers
session.headers = {
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'api.eloverblik.dk',
    'User-Agent': 'Eloverblik-Python'
}

# Gets a saved data token if it is not too old, alternatively gets a new token
def get_or_set_data_access_token(token):
    # If an existing data access token is less than 12 hours old, use it and return
    if exists(data_access_token_filename):
        with open(data_access_token_filename, 'rb') as data_access_token_file:
            save_time_and_token = pickle.load(data_access_token_file)
            if not datetime.now() - save_time_and_token[0] > timedelta(hours=12):
                print('Existing data access token found. Using this token.')
                session.headers['Authorization'] = 'Bearer ' + save_time_and_token[1]
                return
    # Data access token does not exist or is too old
    # Check whether API is alive
    print('Checking API status...')
    get_api_status = get_endpoint('isalive')
    if get_api_status == True:
        print('API reports that it is up')
        # Get data access token for subsequent requests
        print('Getting data access token...')
        session.headers['Authorization'] = 'Bearer ' + token
        token_get_time = datetime.now()
        get_data_access_token = get_endpoint('token')
        # Request succesful
        if get_data_access_token:
            print('Got data access token')
            data_access_token = get_data_access_token['result']
            # Save token to file with get time
            with open(data_access_token_filename, 'wb') as data_access_token_file:
                pickle.dump([token_get_time, data_access_token], data_access_token_file)
            session.headers['Authorization'] = 'Bearer ' + data_access_token
        # Request failed
        else:
            sys.exit('Error: Unable to get data access token. Exiting.')
    # API is down
    else:
        sys.exit('Error: API is down. Exiting.')

# Request an endpoint and return data
def get_endpoint(endpoint, json=None):
    tries = 1
    while tries <= api_retries:
        if not json:
            response = session.get(base_url + endpoint, timeout=10)
        else:
            response = session.post(base_url + endpoint, json=json, timeout=10)
        # Succesful request
        if response.status_code == 200:
            return response.json()
        # Unsuccesful request, try again after 1 second
        elif response.status_code == 429 or response.status_code == 503:
            tries += 1
            time.sleep(1)
        else:
            print(f'API returned an unknown status code')
            print(f'Latest API response status code was: {response.status_code}')
            print(f'Latest API response content was: {response.text}')
            sys.exit('API request failed. Exiting.')
    if tries > api_retries:
        print(f'API request did not succeed after {api_retries} attempts')
        print(f'Latest API response status code was: {response.status_code}')
        print(f'Latest API response content was: {response.text}')
        sys.exit('API request failed. Exiting.')

# Lists all metering points
def list_meters():
    print('Getting list of meters...')
    get_metering_points = get_endpoint('meteringpoints/meteringpoints')
    print(f'Found {len(get_metering_points["result"])} meter(s)')
    print('Printing list of meter(s)...\n')
    for meter in get_metering_points['result']:
        meter_count = 1
        print(f'--- Meter {meter_count} ---')
        for key, value in meter.items():
            print(key, ':', value)
        print('---')
        meter_count += 1
    sys.exit('All meters printed. Exiting.')

# Gets and saves metering point electricity usage data as a csv file
def get_usage_data(meter_ids, args, periods):
    print('Starting to save usage data...')
    # Prepare csv file for writing
    with open('eloverblik_usage_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['meter_id', 'resolution', 'timestart_utc', 'timestart_denmark', 'timeend_utc', 'timeend_denmark', 'point_position', 'point_out_quantity', 'point_out_quality']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for meter_id in meter_ids:
            print(f'Getting and saving usage data for meter id {meter_id}...')
            meter_json = {
                "meteringPoints": {
                    "meteringPoint": [
                        meter_id
                    ]
                }
            }
            for date_period in periods:
                print(f'Saving usage date for period {date_period[0]} to {date_period[1]}...')
                usage_data_endpoint = 'meterdata/gettimeseries/' + date_period[0] + '/' + date_period[1] + '/' + args.aggregation
                get_meter_usage_data = get_endpoint(usage_data_endpoint, meter_json)
                for result in get_meter_usage_data['result']:
                    for time_serie in result['MyEnergyData_MarketDocument']['TimeSeries']:
                        for period in time_serie['Period']:
                            resolution = period['resolution']
                            timestart_utc = period['timeInterval']['start']
                            timestart_datetime = datetime.strptime(timestart_utc, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo('UTC'))
                            timestart_denmark = timestart_datetime.astimezone(ZoneInfo('Europe/Copenhagen'))
                            timestart_denmark_str = datetime.strftime(timestart_denmark, '%Y-%m-%dT%H:%M:%S')
                            timeend_utc = period['timeInterval']['end']
                            timeend_datetime = datetime.strptime(timeend_utc, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo('UTC'))
                            timeend_denmark = timeend_datetime.astimezone(ZoneInfo('Europe/Copenhagen'))
                            timeend_denmark_str = datetime.strftime(timeend_denmark, '%Y-%m-%dT%H:%M:%S')
                            period_rows = [
                                {
                                    'meter_id': meter_id,
                                    'resolution': resolution,
                                    'timestart_utc': timestart_utc,
                                    'timestart_denmark': timestart_denmark_str,
                                    'timeend_utc': timeend_utc,
                                    'timeend_denmark': timeend_denmark_str,
                                    'point_position': point['position'],
                                    'point_out_quantity': str(point['out_Quantity.quantity']).replace('.',','),
                                    'point_out_quality': point['out_Quantity.quality']
                                }
                                for point in period['Point']
                            ]
                            writer.writerows(period_rows)
                print(f'Saved usage date for period {date_period[0]} to {date_period[1]}')
            print(f'Saved usage data for meter {meter_id}')
        print(f'Saved usage data for meter(s)')    

# Gets and saves metering point electricity charges data as a csv file
def get_charges_data(meter_ids):
    print('Starting to save charges data...')
    # Prepare csv file for writing
    with open('eloverblik_charges_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['meter_id', 'chargetype', 'name', 'description', 'owner', 'validfromdate', 'validtodate', 'periodtype', 'position', 'price', 'quantity']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for meter_id in meter_ids:
            print(f'Getting and saving charges data for meter id {meter_id}...')
            meter_json = {
                "meteringPoints": {
                    "meteringPoint": [
                        meter_id
                    ]
                }
            }
            charges_data_endpoint = 'meteringpoints/meteringpoint/getcharges'
            get_meter_charges_data = get_endpoint(charges_data_endpoint, meter_json)
            for result in get_meter_charges_data['result']:
                for item in result['result']['fees']:
                    chargetype = 'fee'
                    subscription_row = {
                        'meter_id': meter_id,
                        'chargetype': chargetype,
                        'name': item['name'],
                        'description': item['description'],
                        'owner': item['owner'],
                        'validfromdate': item['validFromDate'],
                        'validtodate': item['validToDate'],
                        'periodtype': item['periodType'],
                        'position': '',
                        'price': str(item['price']).replace('.',','),
                        'quantity': item['quantity']
                    }
                    writer.writerow(subscription_row)
                for item in result['result']['subscriptions']:
                    chargetype = 'subscription'
                    subscription_row = {
                        'meter_id': meter_id,
                        'chargetype': chargetype,
                        'name': item['name'],
                        'description': item['description'],
                        'owner': item['owner'],
                        'validfromdate': item['validFromDate'],
                        'validtodate': item['validToDate'],
                        'periodtype': item['periodType'],
                        'position': '',
                        'price': str(item['price']).replace('.',','),
                        'quantity': item['quantity']
                    }
                    writer.writerow(subscription_row)
                for item in result['result']['tariffs']:
                    chargetype = 'tariff'
                    name = item['name']
                    description = item['description']
                    owner = item['owner']
                    validfromdate = item['validFromDate']
                    validtodate = item['validToDate']
                    periodtype = item['periodType']
                    tariff_rows = [
                        {
                            'meter_id': meter_id,
                            'chargetype': chargetype,
                            'name': name,
                            'description': description,
                            'owner': owner,
                            'validfromdate': validfromdate,
                            'validtodate': validtodate,
                            'periodtype': periodtype,
                            'position': point['position'],
                            'price': str(point['price']).replace('.',','),
                            'quantity': ''
                        }
                        for point in item['prices']
                    ]
                    writer.writerows(tariff_rows)
            print(f'Saved charges data for meter {meter_id}')
        print(f'Saved charges data for meter(s)')      

# Main program logic
def main():
    # Define and load parser arguments
    parser = argparse.ArgumentParser(description='Get data on electricity usage from eloverblik.dk')
    parser.add_argument('-m', '--mode', help='Mode: List meters or get data ', type=str, choices=['list', 'get'], required=True)
    parser.add_argument('-n', '--meterid', help='Get data from this specific meter in get mode', type=str)
    parser.add_argument('-a', '--aggregation', help='Get timeseries data with this aggregation in get mode', choices=['Actual', 'Quarter', 'Hour', 'Day', 'Month', 'Year'], default='Actual', type=str)
    parser.add_argument('-f', '--fromdate', help='Get data from this date in get mode, format yyyy-mm-dd', type=str)
    parser.add_argument('-t', '--todate', help='Get data to and including this date in get mode, format yyyy-mm-dd', type=str)
    parser.add_argument('-d', '--deletetoken', help='Delete existing token file', action='store_true')
    parser.add_argument('-r', '--refreshdatatoken', help='Force refresh of data access token by deleting token file', action='store_true')
    args = parser.parse_args()

    # Delete token file if set as argument
    if args.deletetoken:
        print('Deleting existing token file if it exists')
        os.remove(token_filename)

    # Delete data token file if set as argument
    if args.refreshdatatoken:
        print('Deleting existing data access token file if it exists')
        os.remove(data_access_token_filename)

    # Load or save token
    if not exists(token_filename):
        print('No token from eloverblik.dk saved. Paste your token here.')
        token = str(input('Token: '))
        with open(token_filename, 'wb') as token_file:
            pickle.dump(token, token_file)
    else:
        with open(token_filename, 'rb') as token_file:
            token = pickle.load(token_file)
  
    # If mode is list meters, get a list of meters
    if args.mode == 'list':
        print('Listing available meters...')
        # Get data access token
        get_or_set_data_access_token(token)
        # List meters
        list_meters()
    # If mode is get data, get data
    elif args.mode == 'get':
        # Date argument validation
        if args.fromdate and not args.todate or args.todate and not args.fromdate:
            sys.exit('Error: You must specify both a from date and a to date. Exiting.')
        try:
            from_date = datetime.strptime(args.fromdate, '%Y-%m-%d').date()
            to_date = datetime.strptime(args.todate, '%Y-%m-%d').date()
            if from_date > to_date:
                sys.exit('Error: Your from date cannot be after your to date. Exiting.')
            elif from_date == to_date:
                sys.exit('Error: Your from date cannot be the same as your to date. Exiting.')
            elif from_date > today:
                sys.exit('Error: Your from date cannot be after today. Exiting.')
            elif to_date > today + timedelta(days=1):
                sys.exit('Error: Your to date cannot be later than one day after today. Exiting.')
        except ValueError:
            sys.exit('Error: From or to date in invalid format. Format must be yyyy-mm-dd with no quotes. Exiting.')

        # Periods must be a maximum of 730 days, so longer periods are sliced into smaller pieces
        if to_date > from_date + timedelta(days=730):
            periods = []
            start_of_period = from_date
            slice_finished = False
            while slice_finished == False:
                end_of_period = start_of_period + timedelta(days=730)
                if end_of_period <= to_date:
                    periods.append([datetime.strftime(start_of_period, '%Y-%m-%d'), datetime.strftime(end_of_period, '%Y-%m-%d')])
                    start_of_period = end_of_period + timedelta(days=1)
                else:
                    end_of_period = to_date
                    periods.append([datetime.strftime(start_of_period, '%Y-%m-%d'), datetime.strftime(end_of_period, '%Y-%m-%d')])
                    slice_finished = True
        # Smaller periods are saved as a list in a list to use the same for loop later
        else:
            periods = [[args.fromdate, args.todate]]

        print('Getting data...')

        # Get data access token
        get_or_set_data_access_token(token)

        # Specifik meter id is set by user
        if args.meterid:
            meter_ids = [args.meterid]
        # Meter id argument is not set, so list of meters is fetched and listed
        else:
            # Get ids of meters
            print('Getting list of meters...')
            get_metering_points = get_endpoint('meteringpoints/meteringpoints')
            print(f'Found {len(get_metering_points["result"])} meters')
            meter_ids = [meter['meteringPointId'] for meter in get_metering_points['result']]

        if meter_ids:
            # Get data from meters
            print('Getting and saving usage and charges data for meter(s)...')
            # Get usage data
            get_usage_data(meter_ids, args, periods)
            # Get charges data
            get_charges_data(meter_ids)
            # Print status
            print('Saved usage and charges data for meter(s)')
        else:
            sys.exit('Error: Did not find any meters, so no data to fetch. Exiting.')

if __name__ == '__main__':
    main()