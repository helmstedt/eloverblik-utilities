Utilities for fetching energy data in/from Denmark
============================================

This is a utility for fetching data from https://eloverblik.dk using an API token generated on that site. Also, an utility is available for getting emissions data from https://energidataservice.dk.

Run `python eloverblik.py -h` for information about command line options. Have your token ready.

Getting started
=========

To get started, you can list your meters by running:

`python eloverblik.py -m list`

The script will prompt you for your token. Paste your token and press ENTER. Your token is now saved.

Next, you can download your usage and charges data for all your meters for January 2024 by running:

`python eloverblik.py -m get -f 2024-01-01 -t 2024-01-31`

If you want to limit your data extract to a single meter, add the meter id to your command like this (replace the number at the end with your actual meter id):

`python eloverblik.py -m get -f 2024-01-01 -t 2024-01-31 -n 572318191105164509`

Questions
=========

Feel free to get in touch. My contact information is available on https://helmstedt.dk.