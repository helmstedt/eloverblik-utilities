# A utility to interact with the api from eloverblik.dk
# By Morten Helmstedt, https://helmstedt.dk
# API documentation:
# https://api.eloverblik.dk/CustomerApi/swagger/index.html
# https://www.niras.dk/media/4vbbvkig/eloverblik-adgang-til-egne-data-via-api-kald-forkortet-1.pdf
import requests
import argparse
import time
import sys
import csv
from os.path import exists

# Set token filename
token_filename = 'eloverblik.token'

# Number of API retries (API often returns 503 errors)
api_retries = 10

# API base url
base_url = 'https://api.eloverblik.dk/CustomerApi/api/'

# Prepare session for requests
session = requests.Session()

# Set session headers
session.headers = {
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'api.eloverblik.dk',
    'User-Agent': 'Eloverblik-Python'
}

def get_and_set_data_access_token(token):
    # Check whether API is alive
    print('Checking API status...')
    get_api_status = get_endpoint('isalive')
    if get_api_status == True:
        print('API reports that it is up')
        # Get data access token for subsequent requests
        print('Getting data access token...')
        session.headers['Authorization'] = 'Bearer ' + token
        get_data_access_token = get_endpoint('token')
        if get_data_access_token:
            print('Got data access token')
            data_access_token = get_data_access_token['result']
            session.headers['Authorization'] = 'Bearer ' + data_access_token
        else:
            sys.exit('Unable to get data access token. Exiting.')
    else:
        sys.exit('API is down. Exiting.')

def get_endpoint(endpoint, json=None):
    tries = 1
    while tries <= api_retries:
        if not json:
            response = session.get(base_url + endpoint)
        else:
            response = session.post(base_url + endpoint, json=json)
        # Succesful request
        if response.status_code == 200:
            return response.json()
        # Unsuccesful request, try again after 1 second
        else:
            tries += 1
            time.sleep(1)
    if tries > api_retries:
        print(f'API request did not succeed after {api_retries} attempts')
        return False

def main():
    # Set and load parser arguments
    parser = argparse.ArgumentParser(description='Get data on electricity usage from eloverblik.dk')
    parser.add_argument('-m', '--mode', help='Mode: List meters or get data ', type=str, choices=['list', 'get'], required=True)
    parser.add_argument('-n', '--meterid', help='Get data from this specific meter in get mode', type=str)
    parser.add_argument('-a', '--aggregation', help='Get timeseries data with this aggregation in get mode', choices=['Actual', 'Quarter', 'Hour', 'Day', 'Month', 'Year'], default='Actual', type=str)
    parser.add_argument('-f', '--fromdate', help='Get data from this date in get mode, format yyyy-mm-dd', type=str)
    parser.add_argument('-t', '--todate', help='Get data to and including this date in get mode, format yyyy-mm-dd', type=str)
    parser.add_argument('-d', '--deletetoken', help='Delete existing token file', action='store_true')
    args = parser.parse_args()

    # Delete token file if set as argument
    if args.deletetoken:
        print('Deleting existing token file')
        os.remove(token_filename)

    # Load or save token
    if not exists(token_filename):
        print('No token from eloverblik.dk saved. Paste your token here.')
        token = str(input('Token: '))
        with open(token_filename, 'wt') as token_file:
            token_file.write(token)
    else:
        with open(token_filename, 'rt') as token_file:
            token = token_file.readline()
    
    # TODO: Date argument validation
    
    # If mode is list meters, get a list of meters
    if args.mode == 'list':
        print('Listing available meters...')
        # Get data access token
        get_and_set_data_access_token(token)
        # Get ids of meters
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
    # If mode is get data, get data
    elif args.mode == 'get':
        if args.fromdate and args.todate:
            print('Getting data...')
            # Get data access token
            get_and_set_data_access_token(token)
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
                print('Getting and saving data from meter(s)...')
                # Prepare csv file for writing
                with open('eloverblik_data.csv', 'w', newline='') as csvfile:
                    fieldnames = ['meter_id', 'resolution', 'timestart_utc', 'timeend_utc', 'point_position', 'point_out_quantity', 'point_out_quality']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for meter_id in meter_ids:
                        print(f'Getting and saving meter data for meter id {meter_id}...')
                        meter_json = {
                            "meteringPoints": {
                                "meteringPoint": [
                                    meter_id
                                ]
                            }
                        }
                        usage_data_endpoint = 'meterdata/gettimeseries/' + args.fromdate + '/' + args.todate + '/' + args.aggregation
                        get_meter_usage_data = get_endpoint(usage_data_endpoint, meter_json)
                        for result in get_meter_usage_data['result']:
                            for time_serie in result['MyEnergyData_MarketDocument']['TimeSeries']:
                                for period in time_serie['Period']:
                                    resolution = period['resolution']
                                    # TODO: Convert to local time
                                    timestart_utc = period['timeInterval']['start']
                                    timeend_utc = period['timeInterval']['end']
                                    period_rows = [
                                        {
                                        'meter_id': meter_id,
                                        'resolution': resolution,
                                        'timestart_utc': timestart_utc,
                                        'timeend_utc': timeend_utc,
                                        'point_position': point['position'],
                                        'point_out_quantity': point['out_Quantity.quantity'],
                                        'point_out_quality': point['out_Quantity.quality'],
                                        }
                                        for point in period['Point']
                                    ]
                                    writer.writerows(period_rows)
            else:
                sys.exit('Did not find any meters, so no data to fetch. Exiting.')
        else:
            sys.exit('You must specify both a from date and a to date. Exiting.')

if __name__ == '__main__':
    main()