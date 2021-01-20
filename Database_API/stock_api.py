# Importing the data extraction and management packages:
import sqlite3
import pandas as pd
import backtrader as bt


class StockData(object):
    """An Object that contains all of the methods and internal
    params for interacting and querying stock data from a database
    maintained by the 'ETL_pipleines.stock_pipeline' APIs.

    The method is specifically designed to work with databases maintained
    by my ETL Pipleine API as it makes explicit assumptions about database schema
    that cannot be externally configured (for simplicity).

    Like the ETL API itself this object is just a glorified wrapper around the pandas
    query API, they are the real heroes here. The object exists to simplify data extraction
    and ingestion to other applications such as a backtrader backtesting engine.

    Arguments:
        dbpath (str): The path to the sqlite database that the object connects to.

    """
    def __init__(self, dbpath):
        # Declaring instance params:
        self.dbpath = dbpath 
        self.con = sqlite3.connect(self.dbpath)
    
    def get_ohlc_df(self, ticker):
        """Method makes use of the pandas.read_sql_query method to
        query the Open, High, Low, Close dataframe from the database 
        table "{TICKER}_ohlc".

        The method is a basic wrapper for the pandas method.

        Arguments:
            ticker (str): The ticker string that will be used to point to 
                the correct database table.

        Returns:
            pd.Dataframe: The formatted dataframe containing the OHLC pricing
                data for the specific ticker.

            None: None type if the `read_sql_query` pandas method fails.

        """
        try:
            # Querying Database via Pandas for Open High Low Close Dataframe:
            ohlc_df = pd.read_sql_query(
                f"SELECT * FROM {ticker}_ohlc",
                self.con,
                parse_dates= ["Date"],
                index_col="Date")
                
            return ohlc_df

        except:
            return None
        
    def get_ohlc_datafeed(self, ticker):
        """Method extracts a formatted OHLC price dataframe from the database
        and wraps it into a backtrader PandasData data feed.

        This method is used to provide an easy way to feed price data from the
        database into a backtrader backtest Cerebro engine.
        """
        # Querying Database for Open High Low Close Dataframe:
        ohlc_df = self.get_ohlc_df(ticker)

        if ohlc_df is None:
            return ohlc_df

        else:
            # Passing the formatted dataframe into a backtrader datafeed:
            return bt.feeds.PandasData(dataname=ohlc_df)
