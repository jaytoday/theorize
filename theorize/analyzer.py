import pandas as pd
from typing import List, Tuple

class Analyzer():
    """
    Class to encapsulate swap analysis operations
    """

    def __init__(self):
        pass

    def get_accounts(self, swaps: pd.DataFrame, token_list: List[Tuple[str, float]]) -> List[str]:
        """
        Returns accounts which are senders of the given tokens and where
        tokens are acquired above the given threshold

        :param List[Tuple[str, float]] token_list: list of tokens and
            their acquired amount thresholds
        """
        # first, calculate total acquired amount of tokens by the senders.
        swaps_grouped = swaps.groupby(
            ["sender", "related_token"]
        )[["acquired_amount"]].sum().reset_index()

        # iterate over tokens
        for token, threshold in token_list:
            # filter out the accounts that acquired below the thresolh
            # of the related token
            swaps_grouped = swaps_grouped[
                (swaps_grouped["related_token"] != token) |
                ((swaps_grouped["related_token"] == token) &
                 (swaps_grouped["acquired_amount"] >= threshold))
            ]

        # return unique list of senders.
        return swaps_grouped["sender"].unique().tolist()

    def get_swap_total_amounts(
            self, last30day_swaps_by_sender: pd.DataFrame) -> pd.DataFrame:
        """
        Given last 30 day swaps of the senders, apply aggregated
        analysis and return the total amountUSD of sender by pair in format
        of sender, token0, token1, total amountUSD and amountUSD_percentage

        :param pd.DataFrame last30day_swaps_by_sender:
            last 30 day swaps by sender
        :return pd.DataFrame: total amountUSD and amountUSD percentage
            by sender and pair
        """
        df = last30day_swaps_by_sender.groupby(
            ["sender", "pair_token0_symbol", "pair_token1_symbol"]
        )[["amountUSD"]].sum().reset_index()
        df["amountUSD_percentage"] = (
            100 * (df["amountUSD"] / df["amountUSD"].sum()))

        return df.sort_values("amountUSD_percentage", ascending=False)
