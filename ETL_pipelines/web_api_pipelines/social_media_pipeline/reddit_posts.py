# Importing Data Manipulation packages:
import pandas as pd
import sqlite3
import bonobo
import os
import json
import requests
from datetime import datetime, timezone
from pytz import timezone

# Python Reddit API Wrapper:
import praw

# Importing the Sqlite Reddit Pipeline Object
from ETL_pipelines.base_pipeline import web_api_json_load
from ETL_pipelines.sqlite_pipelines.social_media_pipeline.reddit_posts import RedditContentPipeline

class RedditContentWebAPIPipeline(RedditContentPipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant subreddit posts to a database through
    the velkozz REST API.

    It inherits from the Sqlite RedditContentPipeline object and is modified
    to perform POST and GET requests to the velkozz Web API through the requests
    python library.

    The primary documentation for this pipeline is found in the Sqlite RedditContentPipeline
    API. This objects adapts the Pipeline object to insert data to a REST Web API
    in place of a Sqlite database.

    Example:
        test_pipeline = RedditContentWebAPIPipeline("http://localhost:8000/apirscience/", "science")
 
     Arguments:
        api_endpoint (str): The API endpoint associated with the specific subreddit database table.
            This is the url endpoint that the requests method uses to Query and Write JSON data to.

        subreddit (str): The string that indicates the specific subreddit
            that the data is to be scraped from.
    """
    def __init__(self, api_endpoint, subreddit_name, **kwargs):

        # Declaring Instance Parameters:
        self.api_endpoint = api_endpoint
        self.subreddit_name = subreddit_name
        self.kwargs = kwargs

        # Creating a reddit praw instance based on specified subreddit:
        # TODO: Add logic to extract praw config from KWARGS instead of env params.
        self.reddit = praw.Reddit(
            client_id = os.environ["CLIENT_ID"],
            client_secret= os.environ["CLIENT_SECRET"],
            user_agent = os.environ["USER_AGENT"]
        )

        self.subreddit = self.reddit.subreddit(self.subreddit_name)

        print(f"Reddit Instance Initalized with Read Status:{self.reddit.read_only}")

        # Execuring all of the ETL functions mapped in the graph:
        self.execute_pipeline()

    def transform_posts(self, *args):
        """The method recieves a length 1 tuple containing a dict of reddit posts generated 
        from the extraction methods and performs transformation on the dict to convert it 
        into a dataframe of elements that are not already stored in the database.

        The dictionary recieved from the extraction methods are in the format:

        {
            id1: [title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author],
            idn: [title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author]
        }
                    
        The transformation method queries the Web API for existing posts posted on the
        day that the pipeline is executed. It compares the index of the database data 
        and the index of the data recieved from the extraction methods. Only unique
        elements not already in the database are passed into the load method.

        When converting a dictionary to a unique elements Dataframe the method unpacks
        the information and converts all data to the correct data types. 
                
        Yields: DataFrame
            A DataFrame containing all of the relevant information for each submission in 
                the format:

        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        |id (index)| title |content|upvote_ratio|score|num_comments|created_on|stickied|over_18|spoiler|permalink|author|author_is_gold|author_mod|author_has_verified_email|author_created|comment_karma|
        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        | string   | string| string|   float    | int |     int    | datetime |  Bool  | Bool  |  Bool |   str   |  str |      Bool    |  Bool    |           Bool          |      str     |     int     |
        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        """
        # Unpacking Args Tuple:
        posts_dict = args[0]
        
        # Querying the Web API for all subreddit posts:
        key = self.kwargs["API_Key"]
        existing_posts_response = requests.get(self.api_endpoint, headers={"Authorization":f"Token {key}"})
        print(f"Made Request to the Database for {self.subreddit_name} posts with status code {existing_posts_response.status_code}")

        # Conditional that ensures correct get request status code:  
        if existing_posts_response.status_code != 200:
            raise ValueError(f"Response from Web API Request w/ Status Code {existing_posts_responses.status_code}")
        
        # Converting the json response object to a dataframe:
        existing_posts_json = existing_posts_response.json()
        
        # If database contains no entry do not attempt to create df:
        if len(existing_posts_json) > 0:
            existing_posts = pd.DataFrame.from_dict(existing_posts_json)
            existing_posts.set_index("id", inplace=True)

        existing_posts_id = []
        try:
            existing_posts_id = existing_posts.index
        except:
            pass
            
        # Extracting unqiue keys from the posts_dict.keys() that are not present in the existing_post_id:
        unique_id_keys = list(set(posts_dict.keys()) - set(existing_posts_id))
        
        # Logging unique IDs to ensure unique ID logic works as intended:
        print("UNIQUE ID KEYS:", unique_id_keys)
        print("EXISTING POST IDs:", existing_posts_id)

        # Unpacking the "Author" parameter and extending Author derived params to the end of the content
        #  list for each dict key-value pair that is unique (not in the database):
        unique_posts_dict = {

            # Unpacking list for faster appending:
            post_id:self._transform_post_content_lst(content_lst) for post_id, content_lst 
            in posts_dict.items() if post_id in unique_id_keys
            
            }
        
        #print(unique_posts_dict.items())
        
        # Converting Dictionary of Unique Post elements to a dataframe:
        posts_df = pd.DataFrame.from_dict(
            unique_posts_dict,
            orient='index',
            columns=[
                "title", "content", "upvote_ratio", "score", "num_comments", "created_on", "stickied", "over_18",
                "spoiler", "permalink", "author", "author_gold", "mod_status", "verified_email_status", "acc_created_on", 
                "comment_karma"])

        # Formatting the dataframe for final loading:
        # Converting UTC Time dataframe columns to datetime:
        posts_df['created_on'] = posts_df['created_on'].apply(
            lambda x: str(datetime.fromtimestamp(x, tz=timezone("EST"))))

        posts_df['acc_created_on'] = posts_df['acc_created_on'].apply(
            lambda x: str(datetime.fromtimestamp(x, tz=timezone("EST"))))

        posts_df['author'] = posts_df.author.astype(str)
        posts_df.reset_index(inplace=True)
        posts_df.rename(columns={'index':'id'}, inplace=True)

        yield posts_df
         
    def load_posts(self, *args):
        """Method writes the reddit posts dataframe into the database
        through the Web API. 
        
        The reddit posts dataframe that is wrtiten to the database is in the following
        format:
        
        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        |id (index)| title |content|upvote_ratio|score|num_comments|created_on|stickied|over_18|spoiler|permalink|author|author_is_gold|author_mod|author_has_verified_email|author_created|comment_karma|
        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        | string   | string| string|   float    | int |     int    | datetime |  Bool  | Bool  |  Bool |   str   |  str |      Bool    |  Bool    |           Bool          |      str     |     int     |
        +----------+-------+-------+------------+-----+------------+----------+--------+-------+-------+---------+------+--------------+----------+-------------------------+--------------+-------------+
        
        Arguments:
            args (tuple): The arguments passed into the load method by the transform method
                containing the dataframe. 
        
        """
        posts_df = args[0]
        
        # Posting data to the Web API via the generic web api load method:
        web_api_json_load(posts_df, self.api_endpoint, API_Key=self.kwargs["API_Key"])

    def build_graph(self, **options):
        """The method that is used to construct a Bonobo ETL pipeline
        DAG that schedules the following ETL methods:

        - Extraction: extract_daily_top_posts, extract_rising_posts
        - Transformation: transform_posts
        - Loading: load_posts

        Returns: 
            bonobo.Graph: The Bonobo Graph that is declared as an instance
                parameter and that will be executed by the self.execute_pipeline method.
        
        """
        # Building the Graph:
        self.graph = bonobo.Graph()    

        # Creating the main method chain for the graph:
        self.graph.add_chain(
            self.transform_posts,
            self.load_posts,
            _input=None # Input set to None so self.transform_posts does not stat untill params are passed.
        ) 

        # Adding the first leg that extracts the top posts for the day:    
        self.graph.add_chain(
            self.extract_daily_top_posts, 
            _output=self.transform_posts # Output of this extraction method is fed into self.transform_posts.
        )

        # Adding the second leg that extracts the current rising posts:
        self.graph.add_chain(
            self.extract_rising_posts,
            _output= self.transform_posts
        )

        return self.graph

    # Executon method:
    def execute_pipeline(self):
        
        self.bonobo_parser = bonobo.get_argument_parser()
        with bonobo.parse_args(self.bonobo_parser) as options:
            bonobo.run(
                self.build_graph(**options),
                services=self.get_services(**options))
    
    # Internal Data Formatting Method:
    def _transform_post_content_lst(self, lst):
        """Internal method is used to transform the base list of reddit 
        post submissions recived from the extraction methods into a full
        list of params assocaited with the reddit post.

        Method takes a list of base params that is the value associed with
        each key in the dict ingested from the extraction methods:

        id: [title, content, upvote_ratio, score, num_comments, created_on, stickied,, over_18, spoilers, permalink, author]
        
        It then transforms this list to contain additional params by unpacking the 'author'
        param (a Redditor instance). The list is transformed to contain the folowing parameters
        in the following order:

        [
            "title", "content", "upvote_ratio", "score", "num_comments", "created_on", "stickied",
            "over_18, "spoilers", "permalink", "author", "author_gold", "mod_status", "verified_email_status", 
            "acc_created_on", "comment_karma"
        ]

        This method was initally performed by dict comprehension within the main transformation method
        but was moved into an internal callable method to add error catching logic. This method is 
        still called within the main dict comprehension:

        Arguments:
            list (list): A list contaiing all the extracted reddit data in the order described above.

        Returns:
            list: The transformed list with full feature extraction and error-catching as described 
                above.

        """

        # TODO: For Gods sake this is the laziest error-catching I have ever written please make this less horrible:
        try:
            transformed_lst = [
                *lst,
                lst[-1].is_gold,
                lst[-1].is_mod,
                lst[-1].has_verified_email,
                lst[-1].created_utc,
                lst[-1].comment_karma
            ]
        
        except:
            transformed_lst = [*lst,"NaN", "NaN", "NaN","NaN","NaN"]
        
        return transformed_lst