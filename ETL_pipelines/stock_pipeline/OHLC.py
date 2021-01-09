# Importing external libraries:
import pandas as pd
import yfinance as yf
import bonobo
import sqlite3

# Importing the Base Pipeline API Object:
from ETL_pipelines import base_pipeline

class OHLCPipeline(Pipeline):
    """An object that wraps all the logic necessary to create
    a simple ETL pipeline for stock prices extracted from Yahoo
    Finance.

    It inherits from the Pipeline API object with allows the extract,
    transform and load methods to be overwritten but the graph creation
    and pipeline execution methods to be inherited.

    Example:
        OHLCPipeline('database url/database path')
        OHLCPipeline.read_ticker_lst('example_tickers.txt')
        OHLCPipeline.execute_pipeline()

    Arguments:
        dbpath (str): The relative or absoloute database URL pointing to
            the database where stock price data should be written.
    """
    def __init__(self, dbpath):

        # Initalizing the parent method:
        super(Pipeline, self).__init__(dbpath)

        # Declaring instance params:
        self.ticker_lst = []
    
    def read_ticker_lst(self, file_path):
        """The method that opens the file containing ticker
        symbols and reads each symbol into the ticker_lst parameter.

        The method assumes ticker symbols are stored in a csv or txt file.

        Arguments:
            file_path (str): The absoloute or relative path to the file containing
                the ticker symbols.

        """
        self._file_path = file_path

        # Opening file with pandas:
        ticker_file = open(self._file_path, 'rt')
        file_contents = ticker_file.read()

        # Seperating string into single list elements: 
        split_ticker_str = file_contents.split("\n")
        
        # Assigning split ticker list to the main param:
        self.ticker_lst = split_ticker_str
        
        ticker_file.close()
    
    # <-----------Bonobo ETL Methods----------->
    def extract(self):
        """Method initalizes the yfinance ticker object to download
        all of the price history data for each ticker in the ticker_lst
        param.

        It iterates through the multi-index dataframe and seperates
        each into dataframes for the ohlc of their specific tickers.
        Each ticker ohlc is then generated and passed into the transform method.

        Yields: 
        
            tuple: A tuple containing the ticker symbol string and the dataframe 
                of a specific ticker symbol extracted from the multi-index dataframe. 
                (str, dataframe)

        """
        # Converting the list of tickers into a single string:
        tickers = " ".join(self.ticker_lst)

        # Performing the price history download from yfinance:
        stock_price_data = yf.download(
            tickers, 
            period='max',
            group_by ='ticker')
        
        # Iterating Through the multi-index dataframe generating individual OHLC dataframe:
        for ticker in self.ticker_lst:
            yield (ticker, stock_price_data[ticker])
    
    def transform(self, *args):
        """Method that ingests the dataframe from the 
        `extract_stock_prices()` method and correctly formats
        data.

        Arguments: 
            args (tuple): The only relevant argument passed into the method
                is the ohlc dataframe for specific ticker and the corresponding ticker symbol:
                (ticker, Dataframe).
        
        Yields:
            tuple: A tuple containing the ticker string and the transformed dataframe. 
        """
        ticker, ohlc = args[0], args[1]

        # Performing formatting transformation on each dataframe:
        ohlc.dropna(inplace=True)

        yield (ticker, ohlc)
    
    def load(self, *args):
        """Method that uses the pandas.to_sql method to
        wrtie each OHLC dataframe to a database.

        """
        # Unpacking argument tuple:
        ticker, price_df = args[0], args[1]

        # Creating sqlite connection here, ensuring con object is in same thread:
        self._con = sqlite3.connect(self.dbpath)

        # Writing price data to the database:
        price_df.to_sql(ticker, self._con, if_exists='replace')


# Example:
OHLCPipeline("test.sqlite")
OHLCPipeline.read_ticker_lst('example.txt')
OHLCPipeline.execute_pipeline()

con = sqlite3.connect('test.sqlite')
test = pd.read_sql_query('SELECT * FROM SPY', con)
print(test)