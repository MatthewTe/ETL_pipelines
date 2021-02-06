# Importing data management libraries:
import pandas as pd
import yfinance as yf
import sqlite3
import json

# Importing ETL libraries:
import bonobo

# Importing webscraping packages:
import requests 
from bs4 import BeautifulSoup 

# Importing the Base Pipeline API Object:
from ETL_pipelines.base_pipeline import Pipeline

# Importing the Sqlite Pipeline API Objects to Re-Factor:
from ETL_pipelines.sqlite_pipelines.stock_pipeline.market_indicies import *

class SPYCompositionWebAPIPipeline(SPYCompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant SPY Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite SPYCompositionPipeline object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite SPYCompositionPipeline
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = SPYCompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.

    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.spy_comp_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing SPY
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)

        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)

class DJIACompositionWebAPIPipeline(DJIACompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant DJIA Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite DJIACompositionPipeline object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite DJIACompositionPipeline
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = DJIACompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.
    
    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.djia_comp_url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing DJIA
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)

        print(parsed_content)

        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)

class SPTSXCompositionWebAPIPipeline(SPTSXCompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant SPTSX Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite SPTSXComposition object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite SPTSXComposition
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = DJIACompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.
    
    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.sptsx_composite_url = "https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index"   

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing SPTSX
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)
        print(parsed_content)
        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)

class FTSECompositionWebAPIPipeline(FTSECompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant FTSE Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite FTSEComposition object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite FTSEComposition
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = FTSECompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.
    
    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.ftse_market_index_url = "https://en.wikipedia.org/wiki/FTSE_100_Index"   

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing FTSE
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)

        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)

class SMICompositionWebAPIPipeline(SMICompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant Swiss Market Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite SMIComposition object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite SMIComposition
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = SMICompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.
    
    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.smi_composition_url = "https://en.wikipedia.org/wiki/Swiss_Market_Index"   

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing SMI
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)

        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)

class SPICompositionWebAPIPipeline(SPICompositionPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant Swiss Performance Index data to a database through
    the velkozz REST API.

    It inherits from the Sqlite SPIComposition object and is modified
    to perform POST requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite SPIComposition
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = SPICompositionWebAPIPipeline("http://localhost:8000/exampleapi/")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.
    
    """
    def __init__(self, api_endpoint, **kwargs):

        # Declaring instance params:
        self.api_endpoint = api_endpoint
        
        # Hard Coded URL for S&P 500 Index Contents:
        self.spi_composition_url = "https://en.wikipedia.org/wiki/Swiss_Performance_Index"   

        # Executing the pipeline:
        self.execute_pipeline() 

    # Overwriting the ETL load method to perform POST request:
    def load(self, *args):
        """The method recieves the formatted dataframe containing SPI
        composition data and loads the data to the velkozz web API.

        The method seralizes the dataframe into a JSON format and sends
        data to the REST API through a POST request.

        Arguments:
            args (tuple): A length-1 tuple containing the formatted
                dataframe of components generted by the transform method
        """
        # Unpacking the argument tuples:
        content_df = args[0]

        # Seralizing Dataframe into a JSON format:
        posts_json = content_df.to_json(orient="records")
        parsed_content = json.loads(posts_json)

        # Making a POST request to the Web API to write data to the database:
        post_response = requests.post(self.api_endpoint, json=parsed_content)
