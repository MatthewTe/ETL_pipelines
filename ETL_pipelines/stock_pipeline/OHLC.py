# Importing external libraries:
import pandas as pd
import yfinance as yf
import bonobo
import sqlite3


class OHLCPipeline():
    """An object that wraps all the logic necessary to create
    a simple ETL pipeline for stock prices extracted from Yahoo
    Finance.

    Arguments:
        dbpath (str): The relative or absoloute database URL pointing to
            the database where stock price data should be written.ß
    """
    def __init__(self, dbpath):
        # Declaring instance params:
        self.ticker_lst = []
        self.dbpath = dbpath
    
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
    def extract_stock_prices(self):
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
    
    def transform_stock_data(self, *args):
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
    
    def load_price_data(self, *args):
        """Method that uses the pandas.to_sql method to
        wrtie each OHLC dataframe to a database.

        """
        # Unpacking argument tuple:
        ticker, price_df = args[0], args[1]

        # Creating sqlite connection here, ensuring con object is in same thread:
        self._con = sqlite3.connect(self.dbpath)

        # Writing price data to the database:
        price_df.to_sql(ticker, self._con, if_exists='replace')

   
    def build_graph(self, **options):
        """The method that builds the bonobo graph from
        the extract_stock_prices, transform_stock_data and load_price_data
        methods

        """
        # Building the Graph:
        self.graph = bonobo.Graph()    
        self.graph.add_chain(
            self.extract_stock_prices,
            self.transform_stock_data,
            self.load_price_data)

        return self.graph

    def get_services(self, **options):
        return {}

    # Executon method:
    def execute_pipeline(self):
        """Method that executes the bonobo graph and performs all of logic described in the
        ETL pipeline.

        Example:
            OHLCPipeline('database url/database path')
            OHLCPipeline.read_ticker_lst('example_tickers.txt')
            OHLCPipeline.execute_pipeline()

        """
        self.bonobo_parser = bonobo.get_argument_parser()
        with bonobo.parse_args(self.bonobo_parser) as options:
            bonobo.run(
                self.build_graph(**options),
                services=self.get_services(**options))