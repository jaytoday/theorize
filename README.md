# Theorize: Data Analysis of Onchain Trading Activity
The Theorize library analyzes onchain trading activity indexed via the [Graph protocol](https://www.thegraph.com).

The main focus of the Theorize library is initially on querying and analyzing both historical and recent trading activity to answer variations of the following question:

*What assets are now being accumulated by accounts that previously accumulated other specified assets during certain time periods?*

This type of analysis can be used to assist with scouting potential investments, by finding the assets that are unusually popular with accounts that have demonstrated a previous ability to identify top performing assets.

The library will be expanded to facilitate other types of analysis of onchain activity that can assist with other strategies, such as identifying market trends.

## Installation

Install with `python setup.py install` or `pip install -r requirements.txt`.

## Usage

Theorize can be run from the command line with `python theorize` or imported as a library. The main function takes the following arguments:
* `tokenList`  - A JSON formatted list of DeFi asset symbols, and the minimum units that an account should have acquired between the specified historical start time and end time.  
* `startTime` - The start time to use for querying historical trading activity, in `%Y-%m-%d %H:%M:%S` format.
*  `endTime` - The end time to use for querying historical trading activity, in `%Y-%m-%d %H:%M:%S` format.

For example, by running `python theorize -tokenList ‘[[“AAVE”, 100], [“SNX”, 200], [“REN”, 10000]]’ -startTime '2021-01-01 00:00:00' -endTime '2021-01-02 00:00:00'` the analysis would retrieve accounts that had retrieved either 100 units of AAVE, 200 units of SNX, or 10000 units of REN on January 1st 2021, and would calculate the assets acquired most (in terms of USD value) in the last 30 days.  

Use `python theorize -h` or `python theorize --help` for instructions on using the Theorize script.
