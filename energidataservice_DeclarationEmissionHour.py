# A utility to interact with the api from energidataservice.dk to download CO2 emission data
# By Morten Helmstedt, https://helmstedt.dk
import argparse
import csv
from datetime import date, datetime, timedelta
import requests
import sys

session = requests.Session()
base_url = 'https://api.energidataservice.dk/dataset/DeclarationEmissionHour'
params = {
    'limit': 5000,
    'offset': 0,
    'sort': 'HourUTC ASC',
    'timezone': 'dk'
}
fieldnames = [
    'HourUTC',
    'HourDK',
    'PriceArea',
    'FuelAllocationMethod',
    'Edition',
    'CO2originPerkWh',
    'CO2PerkWh',
    'SO2PerkWh',
    'NOxPerkWh',
    'NMvocPerkWh',
    'CH4PerkWh',
    'COPerkWh',
    'N2OPerkWh',
    'ParticlesPerkWh',
    'CoalFlyAshPerkWh',
    'CoalSlagPerkWh',
    'DesulpPerkWh',
    'FuelGasWastePerkWh',
    'BioashPerkWh',
    'WasteSlagPerkWh',
    'RadioactiveWastePerkWh',
]
today = date.today()

def create_csv_file_and_save_headers(filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

def save_response_data(filename, response_data):
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerows(response_data['records'])
        print(f"Saved {len(response_data['records'])} records")

def request_records(filename, params):
    response = session.get(base_url, params=params, timeout=10)
    if response.status_code == 200:
        response_data = response.json()
        print(f"Found a total of {response_data['total']} records")
        save_response_data(filename, response_data)
        total = response_data['total']
        error = False
        completed = False
        while params['offset'] < total and error == False and completed == False:
            params['offset'] += params['limit']
            response = session.get(base_url, params=params, timeout=10)
            if response.status_code == 200:
                response_data = response.json()
                if response_data['records']:
                    save_response_data(filename, response_data)
                else:
                    completed = True
            else:
                error = True
        if error == True:
            sys.exit('API request failed. Exiting.')
        else:
            print('Downloaded complete dataset')    
    else:
        sys.exit('API request failed. Exiting.')

# Main program logic
def main():
    csv_filename = 'energidataservice_declarationemissionhour.csv'
    # Define and load parser arguments
    parser = argparse.ArgumentParser(description='Get CO2 data from energidataservice.dk')
    parser.add_argument('-m', '--mode', help='Mode: Complete dataset or specific period', type=str, choices=['complete', 'period'], required=True)
    parser.add_argument('-f', '--fromdate', help='Get data from this date in get mode, format yyyy-mm-dd', type=str)
    parser.add_argument('-t', '--todate', help='Get data to and including this date in get mode, format yyyy-mm-dd', type=str)
    args = parser.parse_args()
  
    # If mode is complete, download full dataset
    if args.mode == 'complete':
        print('Downloading complete dataset...')
        create_csv_file_and_save_headers(csv_filename)
        request_records(csv_filename, params)
    # Else, only download specific period
    elif args.mode == 'period':
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
        csv_filename = args.fromdate + '_' + args.todate + '-' + csv_filename
        create_csv_file_and_save_headers(csv_filename)
        params['start'] = args.fromdate + 'T00:00'
        params['end'] = args.todate + 'T00:00'
        request_records(csv_filename, params)

if __name__ == '__main__':
    main()