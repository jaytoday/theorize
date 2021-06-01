import os
import argparse
from typing import List, Tuple, Union
from datetime import datetime
from graph_client import GraphClient
from datastore import DataStore
from analyzer import Analyzer
import utils
import logging

class Theorize():

    def __init__(self, params):
        logging.info("Initialized Theorize")
        self.params = params
        self.client = GraphClient()
        self.analyzer = Analyzer()
        self.tokens = self.client.get_tokens()
        self.client.data_access.save_tokens(self.tokens)
        self.pairs = self.client.get_pairs(self.tokens)
        self.client.data_access.save_pairs(self.pairs)

    def retrieve_accounts(self):
        # list of token name, minimum amount threshold tuples
        logging.info("Getting historical swaps")
        start_date = str(int(utils.datetime_to_unix_timestamp(self.params.startTime)))
        end_date = str(int(utils.datetime_to_unix_timestamp(self.params.endTime)))
        swaps = self.client.get_swaps(start_date, end_date, self.pairs, self.params.tokenList)

        token_total_list = [
            (symbol, amount * 10)
            for (symbol, amount) in self.params.tokenList
        ]
        logging.info("Getting accounts from historical swaps")
        accounts = self.analyzer.get_accounts(swaps, token_total_list)
        logging.info("number of accounts: %s", len(accounts))
        return accounts


    def retrieve_recent_activity(self, accounts):
        logging.info("getting recent swaps filtered by senders from Step 1")
        today = int(datetime.now().timestamp())
        thirty_days_ago = today - (30 * 24 * 60 * 60)  # 30 days
        sender_swaps = self.client.get_swaps_by_senders(
            str(thirty_days_ago), str(today), accounts, self.tokens, self.pairs
        )
        return self.analyzer.get_swap_total_amounts(sender_swaps)

    def run(self):
        accounts = self.retrieve_accounts()
        swap_total_amounts = self.retrieve_recent_activity(accounts)
        # swap total amounts in format of:
        # sender | pair_token0_symbol | pair_token1_symbol | amountUSD | amountUSD_percentage
        swap_total_amounts.to_csv(
            os.path.join(DataStore.DATASTORE_PATH,
                         "last30day_swap_amounts.csv"),
            index=False
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Theorize: Data Analysis of Onchain Trading Activity")
    parser.add_argument("-tokenList",
        help="List of tokens to use for retrieving accounts. Example: [('AAVE', 100), ('SNX', 200), ('REN', 10000)] ",
        default=[('AAVE', 100), ('SNX', 200), ('REN', 10000)])
    parser.add_argument("-startTime", help="Start Time", default='2021-01-01 00:00:00')
    parser.add_argument("-endTime", help="End Time", default='2021-01-02 00:00:00')
    args = parser.parse_args()
    theorize = Theorize(args)
    theorize.run()
