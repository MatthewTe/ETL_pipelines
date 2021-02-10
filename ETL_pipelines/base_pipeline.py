"Script containing the Base framework for the Pipeline API"

# Importing External Packages:
import bonobo
import requests
import pandas as pd
import json

def web_api_json_load(df, url, **kwargs):
    """The method that converts a pandas dataframe to
    a list of json objects and writes json to an online database.

    The method is meant to load data into an online database through
    A REST API. The method seralizes the dataframe into a json format
    and sends the data to a web API through a HTTP POST request via
    the requests module.

    If additional authentication is necessary such as an API key it
    can be passed into the method through the **kwargs argument. 

    Arguments:
        df (pandas.DataFrame): The dataframe containing the data to be
            seralized into a json format.

        url (str): The api end point that will be used to form the
            HTTP POST request to the web api.

    """
    # Converting dataframe into a json object based on each record:
    df_json = df.to_json(orient="records")
    
    # Creating dicts to configure request:
    json_payload = json.loads(df_json)
    headers = {"content-type":"application/json"}

    # Logic for passing API Key to post request:
    if "API_Key" in kwargs:
        key = kwargs["API_Key"]
        headers["Authentication"] = f"Token {key}"

    # Making the Post Request to the Web API:
    post_response = requests.post(
        url, 
        json=json_payload,
        headers=headers
        )


class Pipeline(object):
    """The Base Object representing an ETL pipeline.

    It contains all of the methods necessary to perform ETL functions
    through the Bonobo library. It contains various blueprint methods 
    that are intended to be overwritten by Pipeline objects that extends 
    the base class.

    The methods that are intended to be overwritten are:
    - extract()
    - transform()
    - load()

    The methods that can but do not need to be overwritten:
    - build_graph()
    - get_services()

    Arguments:
        dbpath (str): The relative or absoloute database URL pointing to
            the database where stock price data should be written.
    
    """
    def __init__(self, dbpath):

        # Declaring instance variables:
        self.dbpath = dbpath

    # <------Base Bonobo ETL Methods------->
    def extract(self):
        pass

    def transform(self, *args):
        pass
    
    def load(self, *args):
        pass

    def build_graph(self, **options):

        # Building the Graph:
        self.graph = bonobo.Graph()    
        self.graph.add_chain(
            self.extract,
            self.transform,
            self.load)

        return self.graph

    def get_services(self, **options):
        return {}
        
    # Executon method:
    def execute_pipeline(self):
        
        self.bonobo_parser = bonobo.get_argument_parser()
        with bonobo.parse_args(self.bonobo_parser) as options:
            bonobo.run(
                self.build_graph(**options),
                services=self.get_services(**options))
