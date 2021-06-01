import pandas as pd
from pathlib import Path
import os
import logging

class DataStore():
    """
    Temporary class to mimic a data-access layer
    by using local files.
    """

    DATASTORE_PATH = "temp_datastore"

    def __init__(self):
        # initialize temp datastore, create necessary folders
        # if they don't exist
        Path(self.DATASTORE_PATH).mkdir(parents=True, exist_ok=True)

    def _read(self, path: str) -> pd.DataFrame:
        """
        Reads a dataframe from the given path
        """
        path = os.path.join(self.DATASTORE_PATH, path)
        return pd.read_csv(path, compression="gzip")

    def _write(self, df, path) -> pd.DataFrame:
        """
        Writes the dataframe to the given path
        """
        path = os.path.join(self.DATASTORE_PATH, path)
        df.to_csv(path, compression="gzip", index=False)

    @property
    def tokens(self):
        """
        Returns tokens dataframe
        """
        try:
            return self._read("tokens.gz")
        except Exception as e:
            logging.warning(e, exc_info=False)
            return None

    @property
    def pairs(self):
        """
        Returns pairs dataframe
        """
        try:
            return self._read("pairs.gz")
        except Exception as e:
            logging.warning(e, exc_info=False)
            return None

    def save_tokens(self, tokens):
        """
        Saves tokens to its destination
        """
        self._write(tokens, "tokens.gz")

    def save_pairs(self, pairs):
        """
        Saves pairs to its destination
        """
        self._write(pairs, "pairs.gz")
