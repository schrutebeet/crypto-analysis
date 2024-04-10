
import os
import requests
from pathlib import Path
from typing import Any, Union
from datetime import datetime, timedelta


import numpy as np
import pandas as pd
import yfinance as yf


class DataExtractor:

    DEFAULT_CRYPTO_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=USD"
    DEFAULT_DATA_FOLDER = Path(__file__).parent / "data_files/"
    DEFAULT_INFO_FILE = "crypto_100_info"


    def __init__(self, crypto_ticker: str) -> None:
        # self.config = config
        self.crypto_pair = crypto_ticker + "-USD"
        self.ticker = yf.Ticker(self.crypto_pair)

    def __str__(self) -> str:
        return f"{self.crypto_pair} extractor"

    @staticmethod
    def request_all_crypto_info(api_url: str = DEFAULT_CRYPTO_URL,
                                folder_path: Union[Path, str] = DEFAULT_DATA_FOLDER,
                                file_name: str = DEFAULT_INFO_FILE,
                                save_file: bool = True) -> pd.DataFrame:
        """Get information general information for top 100 cryptos and store it in a pandas df.

        Args:
            api_url (str, optional): URL with the API call. Defaults to DEFAULT_CRYPTO_URL.

        Returns:
            pd.DataFrame: Returns df with generic information on all available cryptos. 
        """
        target_file = DataExtractor.point_to_specific_file(folder_path, file_name)
        last_modified = DataExtractor.check_last_modified_date(target_file)
        if (datetime.now() - last_modified) <= timedelta(days=7):
            crypto_df = pd.read_csv(target_file)
        else:
            r = requests.get(api_url)
            crypto_dict = r.json()
            crypto_df = pd.DataFrame(crypto_dict)
            if save_file:
                crypto_df.to_csv(target_file, index=False)

        return crypto_df

    @staticmethod
    def point_to_specific_file(folder_path: Union[Path, str] = DEFAULT_DATA_FOLDER, 
                          file_name: str = DEFAULT_INFO_FILE) -> Union[Path, str]:
        if isinstance(folder_path, Path):
            target_file = folder_path / Path(f"{file_name}.csv")
        else:
            target_file = folder_path + f"/{file_name}.csv"
        return target_file

    @staticmethod
    def check_last_modified_date(file_path: Union[Path, str], raise_error: bool = False) -> datetime:
        if not os.path.exists(file_path):
            directory, filename = os.path.split(file_path)
            if raise_error:
                raise FileNotFoundError(f"file '{filename}' not found in path '{directory}'.")
            else:
                last_modified = datetime(1, 1, 1)
        else:
            last_modified_int = os.path.getmtime(file_path)
            last_modified = datetime.fromtimestamp(last_modified_int)
        return last_modified
        
    def get_last_3_months(self) -> pd.DataFrame:
        """Get latest three months OHLCV for the given crypto currency.

        Returns:
            pd.DataFrame: Dataframe with quantitative data on the crypto currency.
        """
        return self.ticker.history("3mo")

    def get_last_month(self) -> pd.DataFrame:
        """Get latest month OHLCV for the given crypto currency.

        Returns:
            pd.DataFrame: Dataframe with quantitative data on the crypto currency.
        """
        base_df = self.ticker.history("1mo")
        df = self.get_diff_prices(base_df)
        return df

    @staticmethod
    def get_minmax_position(df_input: pd.DataFrame) -> float:
        """Get relative position of the latest price with respect to the minimum and maximum prices
           for a given period of time specified in the 'df_input' dataframe.

        Args:
            df_input (pd.DataFrame): Dataframe containing OHLCV data.

        Returns:
            float: Value between 0 and 1 representing how close the current price is from the maximum price.
                   The closer to 0, the closer to the minimum. 1 otherwise.
        """
        min_value, max_value = min(df_input['Close']), max(df_input['Close'])
        latest_price = df_input['Close'].iloc[-1]
        minmax_perc = (latest_price - min_value) / (max_value - min_value)
        return minmax_perc

    def get_diff_prices(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Get the absolute and percentage differences between price for 't' and 't - 1'.

        Returns:
            pd.DataFrame: Dataframe containing OHLCV data + abs and perc differences.
        """
        df = input_df.copy()
        df['abs_price_diff'] = df[['Close']].diff()
        df['pct_price_diff'] = df[['Close']].pct_change()
        df['pct_log_price_value'] = np.log(1 + df[['Close']].pct_change())
        return df
