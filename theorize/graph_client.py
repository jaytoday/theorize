import requests
from datastore import DataStore
from typing import List, Tuple
import pandas as pd
import concurrent.futures
import logging

TOTAL_VOLUME_TOKEN_FILTER_COUNT = 100
RECENT_SWAP_AMOUNT_USD_MINIMUM = 1000000

class GraphClient():
    """
    Main class to access uniswap graphQL API
    """

    GRAPH_REQUEST_TIMEOUT = 60
    API = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"

    def __init__(self):
        self.data_access = DataStore()

    def _make_request(self, query: str, variables: dict = {}):
        """
        helper protected method to make a request to the uniswap API
        """
        req = requests.post(
            self.API,
            json={"query": query, "variables": variables},
            timeout=self.GRAPH_REQUEST_TIMEOUT
        )
        return req

    def _retrieve_data_with_paging(
        self,
        query: str,
        entity: str,
        last_id: str = "",
        n_retries: int = 5,
        variables: dict = {}
    ) -> List[dict]:
        """
        Helper method to paginate on the entitites
        """
        records = []
        last_id = last_id or ""
        variables = variables or {}
        # loop through the records till it reaches to the end
        while last_id is not None:
            n_retries_ = n_retries  # start with initial retry values
            # if we have retry left...
            while n_retries_ > 0:
                # make the request. remember to pass variables.
                # set the lastID variable to keep paginating
                variables["lastID"] = last_id
                req = self._make_request(query, variables)
                if req.status_code == 200:  # successfull
                    resp = req.json()
                    # we expect 'data' key in response. in case it is not in,
                    # log the error and stop the process.
                    if "data" not in resp:
                        # there's something wrong with the request. we should retry
                        n_retries_ -= 1
                        logging.warning("Failed req. response: %s", resp)
                        logging.warning("Retries left: %s", n_retries_)
                        continue

                    # store new records in the list
                    new_records = resp["data"][entity]
                    records.extend(new_records)
                    # get the last id and set it so we can continue
                    if new_records:
                        last_id = new_records[-1]["id"]
                    else:
                        # there's no new record, so set last_id to None
                        # so the process could end
                        last_id = None

                    logging.info("len %s %s", entity, len(records))

                    # We're good so far. so break the inner loop
                    # and continue with new last_id
                    break
                else:
                    # there's something wrong with the request. we should retry
                    n_retries_ -= 1
                    logging.warning("Failed req. response: %s", req)
                    logging.warning("Retries left: %s", n_retries_)

        return records

    def get_tokens(
        self,
        force: bool = False,
        first: int = 1000,
        last_id: str = "",
        n_retries: int = 5
    ) -> pd.DataFrame:
        """
        Retrieve tokens from either local datastore or uniswap API and
        returns it as a pandas DataFrame

        If ``force`` is given as True, tokens are retrieved from uniswap API.
        Otherwise, it is tried to retrieve tokens from local datastore first.
        If no token entry found in loal datastore, then tokens are retrieved
        from uniswap API.

        :param bool force: if given as True, the process is forced to fetch
            token data from uniswap API
        :param int first: number of records to fetch from uniswap API at once.
            Defaults to 1000
        :param str last_id: Used as a continuation identifier. If specified,
            records after this id are fetched.
        :param int n_retries: number of retries in case of a failure. 5 is
            default value.
        :return pd.DataFrame: tokens dataframe
        """
        if not force:
            # try to read tokens from datastore
            # return them if they exist
            tokens = self.data_access.tokens
            if tokens is not None:
                return tokens

        # if last_id is passed as None, set it to empty string to
        # keep querying active
        last_id = last_id or ""

        # define the query
        query = """
            query manyTokens ($lastID: String!)
            {
                tokens (
                    first: %s,
                    orderBy: id,
                    orderDirection: asc,
                    where: { id_gt: $lastID  }
                ) {
                    id
                    symbol
                    name
                    tradeVolumeUSD
                    tradeVolume
                    totalSupply
                    untrackedVolumeUSD
                    txCount
                    totalLiquidity
                }
            }
        """ % (first)

        # fetch tokens from API
        tokens = self._retrieve_data_with_paging(
            query, "tokens", last_id=last_id, n_retries=5)

        # convert tokens to pandas dataframe and set correct data types
        tokens_df = pd.DataFrame(tokens)
        tokens_df.totalLiquidity = tokens_df.totalLiquidity.astype("float32")
        tokens_df.totalSupply = tokens_df.totalSupply.astype("float32")
        tokens_df.tradeVolume = tokens_df.tradeVolume.astype("float32")
        tokens_df.tradeVolumeUSD = tokens_df.tradeVolumeUSD.astype("float32")
        tokens_df.txCount = tokens_df.txCount.astype("int")
        tokens_df.untrackedVolumeUSD = tokens_df.untrackedVolumeUSD.astype(
            "float32")

        # for duplicate symbols, keep the ones that have higher tradeVolume value.
        tokens_df["rank"] = tokens_df.groupby(
            "symbol")["tradeVolume"].rank("dense", ascending=False)
        tokens_df = tokens_df[tokens_df["rank"] == 1].drop("rank", axis=1)
        tokens_df = tokens_df.drop_duplicates(subset=["symbol"])

        return tokens_df

    def get_pairs(
        self,
        tokens: pd.DataFrame,
        force: bool = False,
        first: int = 1000,
        last_id: str = "",
        n_retries: int = 5
    ) -> pd.DataFrame:
        """
        Retrieve pairs from either local datastore or uniswap API and
        returns it as a pandas DataFrame

        If ``force`` is given as True, pairs are retrieved from uniswap API.
        Otherwise, it is tried to retrieve pairs from local datastore first.
        If no token entry found in loal datastore, then pairs are retrieved
        from uniswap API.

        :param bool force: if given as True, the process is forced to fetch
            token data from uniswap API
        :param int first: number of records to fetch from uniswap API at once.
            Defaults to 1000
        :param str last_id: Used as a continuation identifier. If specified,
            records after this id are fetched.
        :param int n_retries: number of retries in case of a failure. 5 is
            default value.
        :return pd.DataFrame: pairs dataframe
        """
        if not force:
            pairs = self.data_access.pairs
            if pairs is not None:
                return pairs
        pairs = self.data_access.pairs
        if pairs is not None:
            return pairs

        # list of pairs to contain
        pairs = []
        # if last_id is passed as None, set it to empty string to
        # keep querying active
        last_id = last_id or ""

        # define the query
        query = """
            query manyPairs ($lastID: String!)
            {
                pairs (
                    first: %s,
                    orderBy: id,
                    orderDirection: asc,
                    where: { id_gt: $lastID  }
                ) {
                id
                token0 {
                    id
                    symbol
                }
                token1 {
                    id
                    symbol
                }
                reserve0
                reserve1
                totalSupply
                reserveETH
                reserveUSD
                trackedReserveETH
                token0Price
                token1Price
                volumeToken0
                volumeToken1
                volumeUSD
                untrackedVolumeUSD
                createdAtTimestamp
                }
            }
        """ % (first)

        # fetch pairs from API
        pairs = self._retrieve_data_with_paging(
            query, "pairs", last_id=last_id, n_retries=n_retries)

        # convert pairs list to pandas dataframe
        # and set column data types
        pairs = pd.json_normalize(pairs, sep="_")
        pairs.reserve0 = pairs.reserve0.astype("float32")
        pairs.reserve1 = pairs.reserve1.astype("float32")
        pairs.reserveETH = pairs.reserveETH.astype("float32")
        pairs.reserveUSD = pairs.reserveUSD.astype("float32")
        pairs.token0Price = pairs.token0Price.astype("float32")
        pairs.token1Price = pairs.token1Price.astype("float32")
        pairs.totalSupply = pairs.totalSupply.astype("float32")
        pairs.trackedReserveETH = pairs.trackedReserveETH.astype("float32")
        pairs.untrackedVolumeUSD = pairs.untrackedVolumeUSD.astype("float32")
        pairs.volumeToken0 = pairs.volumeToken0.astype("float32")
        pairs.volumeToken1 = pairs.volumeToken1.astype("float32")
        pairs.volumeUSD = pairs.volumeUSD.astype("float32")

        # to avoid duplicate pairs, keep the ones that have highest reserveUSD
        pairs["rank"] = pairs.groupby(["token0_id", "token1_id"])[
            "reserveUSD"].rank("dense", ascending=False)
        pairs = pairs[pairs["rank"] == 1].drop("rank", axis=1)

        # join pairs with tokens and keep the pairs that have valid tokens
        token_ids = tokens[["id"]].rename(columns={"id": "token_id"})
        pairs = pairs.merge(
            token_ids, left_on="token0_id", right_on="token_id", how="inner"
        ).drop("token_id", axis=1)
        pairs = pairs.merge(
            token_ids, left_on="token1_id", right_on="token_id", how="inner"
        ).drop("token_id", axis=1)
        pairs = pairs.drop_duplicates()

        return pairs

    def _get_swaps(
        self,
        start_time: str,
        end_time: str,
        pair_ids: List[str] = None,
        amount_filter: Tuple[str, str] = None,
        remove_pair_ids: List[str] = None,
        senders: List[str] = None,
        first: int = 500,
        last_id: str = "",
        n_retries: int = 5
    ) -> pd.DataFrame:
        """
        Helper method to retrieve swaps from the uniswap API which builds
        queries with custom filters according to start_time, end_time, pair_ids,
        amount_filter, senders etc.

        :param str start_time: start timestamp for swaps
        :param str end_time: end timestamp for swaps
        :param List pairs: if specified, swaps of those pairs are retrieved.
            Defaults to None.
        :param Tuple[str, str] amount_filter: if specified, amount filter is applied
            on amountX column. Defauls to None
        :param List senders: if specified, swaps of those senders are retrieved.
            Defaults to None.
        :param int first: number of records to fetch from uniswap API at once.
            Defaults to 500
        :param str last_id: Used as a continuation identifier. If specified,
            records after this id are fetched.
        :param int n_retries: number of retries in case of a failure. 5 is
            default value.
        :return pd.DataFrame: dataframe of swaps retrieved
        """
        # Build the query filters...
        where = [
            ("timestamp_gt", start_time),
            ("timestamp_lt", end_time),
            ("id_gt", "$lastID")
        ]
        if pair_ids:
            where.append(("pair_in", "$pairIDs"))
        if senders:
            where.append(("sender_in", "$senders"))
        if remove_pair_ids:
            where.append(("pair_not_in", "$pairNotIDs"))
        if amount_filter:
            where.append(amount_filter)

        # convert the filter to querystring part.
        where_str = ", ".join([": ".join(w) for w in where])
        if where_str:
            where_str = "where: { " + where_str + "}"

        # if last_id is passed as None, set it to empty string to
        # keep querying active
        last_id = last_id or ""

        # build the query
        query = """
          query manySwaps ($lastID: String!, $pairIDs: [String!], $senders: [Bytes!], $pairNotIDs: [String!])
          {
            swaps(
              first: %(first)s,
              orderBy: id,
              orderDirection: asc,
              %(where_str)s,
            ) {
                id
                pair {
                  id
                  token0 {
                    id
                    symbol
                    name
                  }
                  token1 {
                    id
                    symbol
                    name
                  }
                }
                sender
                to
                timestamp
                amountUSD
                amount0In
                amount1In
                amount0Out
                amount1Out
              }
        }
        """ % {
            'first': first,
            'where_str': where_str
        }

        # define variables
        variables = {
            "lastID": last_id,
            "pairIDs": pair_ids,
            "senders": senders,
            "pairNotIDs": remove_pair_ids
        }

        # retrieve swaps
        swaps = self._retrieve_data_with_paging(
            query, "swaps", last_id, n_retries, variables=variables)

        # convert to pandas dataframe and set correct datatypes
        swaps = pd.json_normalize(swaps, sep="_")
        if not len(swaps):
            raise Exception('No swaps returned for query')
        swaps.amount0In = swaps.amount0In.astype("float32")
        swaps.amount0Out = swaps.amount0Out.astype("float32")
        swaps.amount1In = swaps.amount1In.astype("float32")
        swaps.amount1Out = swaps.amount1Out.astype("float32")
        swaps.amountUSD = swaps.amountUSD.astype("float32")

        return swaps

    def get_swaps(
        self,
        start_time: str,
        end_time: str,
        pairs: pd.DataFrame,
        token_values: Tuple[str, float],
        first: int = 500,
        last_id: str = "",
        n_retries: int = 5
    ) -> pd.DataFrame:
        """
        Get swaps of the tokens where tokens have amountOut above
        the specified threshold  between the defined start and end time
        and that have WETH token in its pairs

        :param str start_time: start timestamp for swaps
        :param str end_time: end timestamp for swaps
        :param pd.DataFrame pairs: pandas dataframe of pairs, which will be used
            to select pair ids for filtering
        :param Tuple[str, float] token_values: tuple of tokens and their amountXOut
            thresholds.
        :param int first: number of records to fetch from uniswap API at once.
            Defaults to 500
        :param str last_id: Used as a continuation identifier. If specified,
            records after this id are fetched.
        :param int n_retries: number of retries in case of a failure. 5 is
            default value.
        :return pd.DataFrame: dataframe of swaps retrieved
        """
        swaps = []
        # open the ThreadPool
        executor = concurrent.futures.ThreadPoolExecutor()
        pool = []

        # Loop over tokens so they can be processed paralelly in multi-theads
        for token, amount_thold in token_values:
            amount_filter = None  # placeholder variable for amount filter
            logging.info("Querying for token: %s", token)
            # get pairs of the corresponding token w.r.t. WETH
            valid_pairs = pairs[
                ((pairs["token0_symbol"] == token) & (pairs["token1_symbol"] == 'WETH')) |
                ((pairs["token1_symbol"] == token) &
                 (pairs["token0_symbol"] == 'WETH'))
            ]
            # if there's only one pair, create a filter for amountXOut
            # to narrow down the search space
            if len(valid_pairs) == 1:
                pair_row = valid_pairs.iloc[0]
                which_amount = "amount0Out" if pair_row["token0_symbol"] == token else "amount1Out"
                amount_filter = (f"{which_amount}_gte", str(amount_thold))

            # get all pair ids
            pair_ids = valid_pairs["id"].values.tolist()

            # submit swap fetching job
            pool.append(
                executor.submit(
                    self._get_swaps,
                    start_time,
                    end_time,
                    pair_ids,
                    amount_filter=amount_filter,
                    first=first,
                    last_id=last_id,
                    n_retries=n_retries
                )
            )

        # wait for their responses
        concurrent.futures.wait(pool)

        for i, result in enumerate(pool):
            # get the thread execution result
            swaps_ = result.result()
            # get corresponding token
            token = token_values[i][0]

            # set extra information

            # set the token name so we can know that this swap is retrieved
            # by this token. also, set token position. 0 or 1.
            swaps_["related_token"] = token
            swaps_["related_token_side"] = swaps_.apply(
                lambda row: 0 if row["pair_token0_symbol"] == token else 1,
                axis=1
            )
            # set the acquired amount of this token after the swap.
            swaps_["acquired_amount"] = swaps_.apply(
                lambda row: row["amount0Out"] if row["related_token_side"] == 0 else row["amount1Out"],
                axis=1
            )

            # filter out by threshold
            swaps_ = swaps_[
                ((swaps_["pair_token0_symbol"] == token) & (swaps_["amount0Out"] >= amount_thold)) |
                ((swaps_["pair_token1_symbol"] == token)
                    & (swaps_["amount1Out"] >= amount_thold))
            ]
            swaps.append(swaps_)

        return pd.concat(swaps)

    def get_swaps_by_senders(
        self,
        start_time: str,
        end_time: str,
        senders: List[str],
        tokens: pd.DataFrame,
        pairs: pd.DataFrame,
        chunk_size: int = 6,
        first: int = 500,
        last_id: str = "",
        n_retries: int = 5
    ) -> pd.DataFrame:
        """
        Get swaps between start and end time of related senders. Divides
        senders into chunks and retrieves swaps in parallel threads to speed
        up the process

        :param str start_time: start timestamp for swaps
        :param str end_time: end timestamp for swaps
        :param List senders: id of the senders whose swaps will be retrieved
        :param int chunk_size: number of senders in a chunk. Defaults to 6.
        :param int first: number of records to fetch from uniswap API at once.
            Defaults to 500
        :param str last_id: Used as a continuation identifier. If specified,
            records after this id are fetched.
        :param int n_retries: number of retries in case of a failure. 5 is
            default value.
        :return pd.DataFrame: dataframe of swaps retrieved
        """
        # get top N tokens that have higher tradeVolumeUSD
        tokens = tokens.sort_values("tradeVolumeUSD", ascending=False)
        tokens = tokens.head(TOTAL_VOLUME_TOKEN_FILTER_COUNT)
        token_ids = tokens["id"].values.tolist()

        pairs_to_eliminate = pairs[
            (pairs["token0_id"].isin(token_ids)) &
            (pairs["token1_id"].isin(token_ids))
        ]["id"].values.tolist()

        pairs_to_keep = pairs[
            ~(
                (pairs["token0_id"].isin(token_ids)) &
                (pairs["token1_id"].isin(token_ids))
            )
        ]["id"].values.tolist()

        if len(pairs_to_eliminate) > len(pairs_to_keep):
            pair_filter_kwargs = {"pair_ids": pairs_to_keep}
        else:
            pair_filter_kwargs = {"remove_pair_ids": pairs_to_eliminate}

        # Divide sender list into chunks so they could be processed paralelly
        sender_chunks = [
            senders[i:i + chunk_size]
            for i in range(0, len(senders), chunk_size)
        ]
        swaps = []
        # open the ThreadPool
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # submit parallel threads for each sender chunk
            res = [
                executor.submit(
                    self._get_swaps,
                    start_time,
                    end_time,
                    senders=sender_list,
                    amount_filter=("amountUSD_gte", str(
                        RECENT_SWAP_AMOUNT_USD_MINIMUM)),
                    first=first,
                    last_id=last_id,
                    n_retries=n_retries,
                    **pair_filter_kwargs
                )
                for sender_list in sender_chunks
            ]
            # wait for their responses
            concurrent.futures.wait(res)

            for result in res:
                # for each parallel task, get the result
                # by result() call and append it to
                # swap list
                swaps.append(result.result())

        return pd.concat(swaps)
