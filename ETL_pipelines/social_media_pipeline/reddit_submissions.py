# Importing Data Manipulation packages:
import pandas as pd
import sqlite3
import bonobo
import os

# Python Reddit API Wrapper:
import praw

# Importing the Base Pipeline API Object:
from ETL_pipelines.base_pipeline import Pipeline

class RedditContentPipeline(Pipeline):
    """An object that contains all the logic and methods
    necessary to construct a ETL pipeline for extracting
    and ingesting daily relevant subreddit posts to a database.

    It inherits from the Pipeline API object with allows the extract,
    transform and load methods to be overwritten but the graph creation
    and pipeline execution methods to be inherited.

    The object extracts filings from the "Top" and "Rising" tabs of a subreddit.
    Each of these Tab's context is extracted by a sperate Extraction method which
    are then both fed into a transformation method which normalizes the data into 
    a standard format to be written to the database. 
    
    See graphviz plots of the bonobo graph for a structure outline of how data flows.
    Once again all credit goes to Bonobo and Pandas for the actual heavy lifting.

    Example:
        test_pipeline = EDGARFilingsPipeline("test.sqlite", "learnpython")
 
     Arguments:
        dbpath (str): The relative or absoloute database URL pointing to
            the database where stock price data should be written.

        filings_type (str): The string that indicates the type of SEC
            EDGAR filings that are extracted by the pipeline. This string
            is passed into the url string used to make the request.
    """
    def __init__(self, dbpath, subreddit_name, **kwargs):

        # Initalizing the parent Pipeline object:
        super(RedditContentPipeline, self).__init__(dbpath)
        self.subreddit_name = subreddit_name

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

    def extract_rising_posts(self):
        """Method extracts the current rising reddit submissions from a subreddit
        via the praw API wrapper.

        The generator yields a dictionary containing relevant information extracted
        from each post generated from the subreddit.rising() praw method. All rising posts
        are compiled into this dict that is then passed into a data transformation method.

        The data is compiled into a dict for speed as it is then converted into a dataframe
        in the data transformation method. All seaching and transformation of raw data is done
        prior to it being converted to a dataframe.

        Yields: Dict
            A dictionary containing all the relevant information for each reddit post
                necessary to compile a dataframe:

                {
                    id1: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author],
                    id2: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author],
                                                ...
                    idn: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author]
                }

        """
        posts_dict = {}

        # Iterating through the rising posts constructing and generating dicts:
        for post in self.subreddit.rising():

            # Building the single dict key-value pair:
            post_content_lst = [
                post.title,
                post.selftext,
                post.upvote_ratio,
                post.score,
                post.num_comments,
                post.created_utc,
                post.stickied,
                post.author
            ]

            posts_dict[post.id] = post_content_lst

        yield posts_dict

    def extract_daily_top_posts(self):
        """Method extracts the daily top reddit submissions from a subreddit
        via the praw API wrapper.

        The generator yields a dictionary containing relevant information extracted
        from each post generated from the subreddit.top(day) praw method. All top posts
        are compiled into this dict that is then passed into a data transformation method.

        The data is compiled into a dict for speed as it is then converted into a dataframe
        in the data transformation method. All seaching and transformation of raw data is done
        prior to it being converted to a dataframe.

        Yields: Dict
            A dictionary containing all the relevant information for each reddit post
                necessary to compile a dataframe:

                {
                    id1: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author],
                    id2: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author],
                                                ...
                    idn: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author]
                }

        """
        posts_dict = {}

        # Iterating through the rising posts constructing and generating dicts:
        for post in self.subreddit.top("day"):

            # Building the single dict key-value pair:
            post_content_lst = [
                post.title,
                post.selftext,
                post.upvote_ratio,
                post.score,
                post.num_comments,
                post.created_utc,
                post.stickied,
                post.author
            ]

            posts_dict[post.id] = post_content_lst

        yield posts_dict

    def transform_posts(self, *args):
        """The method recieves a length 1 tuple containing a dict of reddit posts generated 
        from the extraction methods and performs transformation on the dict to convert it 
        into a dataframe of elements that are not already stored in the database.

        The dictionary recieved from the extraction methods are in the format:

        {
            id1: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author],
            idn: [title, content, upvote_ratio, score, num_comments, created_on, stickied, author]
        }
                    
        The transformation method queries the database for existing posts posted on the
        day that the pipeline is executed. It compares the index of the database data 
        and the index of the data recieved from the extraction methods. Only unique
        elements not already in the database are passed into the load method.

        When converting a dictionary to a unique elements Dataframe the method unpacks
        the information and converts all data to the correct data types. 
                
        Yields: DataFrame
            A DataFrame containing all of the relevant information for each submission in 
                the format:

                +----------+-------+-------+------------+-----+------------+-----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
                |id (index)| title |content|upvote_ratio|score|num_comments|created_on|stickied|author|author_is_gold|author_mod|author_has_verified_email|author_created|comment_karma|
                +----------+-------+-------+------------+-----+------------+----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
                | string   | string| string|   float    | int |     int    | datetime |  Bool  |  str |      Bool    |  Bool    |           Bool          |      str     |     int     |
                +----------+-------+-------+------------+-----+------------+----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
        """
        # Unpacking Args Tuple:
        posts_dict = args[0]
        
        # Querying existing posts from the database during current day:
        con = sqlite3.connect(self.dbpath)

        # TODO: Refine SQL Query to only extract data from database from the current day:
        existing_posts_id = []
        try:
            existing_posts = pd.read_sql_query(f"SELECT * FROM {self.subreddit_name}_posts", con, index_col="id")
            existing_posts_id = existing_posts.index
        except:
            pass

        # Extracting unqiue keys from the posts_dict.keys() that are not present in the existing_post_id:
        unique_id_keys = list(set(posts_dict.keys()) - set(existing_posts_id))

        # Unpacking the "Author" parameter and extending Author derived params to the end of the content
        #  list for each dict key-value pair that is unique (not in the database):
        unique_posts_dict = {

            # Unpacking list for faster appending:
            post_id:[
                *content_lst,
                content_lst[-1].is_gold,
                content_lst[-1].is_mod,
                content_lst[-1].has_verified_email,
                content_lst[-1].created_utc,
                content_lst[-1].comment_karma]
                 
            for post_id, content_lst in posts_dict.items() if post_id in unique_id_keys
            }

        # Converting Dictionary of Unique Post elements to a dataframe:
        posts_df = pd.DataFrame.from_dict(
            unique_posts_dict,
            orient='index',
            columns=[
                "title", "content", "upvote_ratio", "score", "num_comments", "created_on", "stickied", "author", "author_gold", 
                "mod_status", "verified_email_status", "acc_created_on", "comment_karma"])

        # Converting 'author' column data type to string:
        posts_df['author'] = posts_df.author.astype(str)

        yield posts_df
         
    def load_posts(self, *args):
        """Method writes the reddit posts dataframe into
        the sqlite database. 
        
        The reddit posts dataframe that is wrtiten to the database is in the following
        format:
        
        +----------+-------+-------+------------+-----+------------+-----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
        |id (index)| title |content|upvote_ratio|score|num_comments|created_on|stickied|author|author_is_gold|author_mod|author_has_verified_email|author_created|comment_karma|
        +----------+-------+-------+------------+-----+------------+----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
        | string   | string| string|   float    | int |     int    | datetime |  Bool  |  str |      Bool    |  Bool    |           Bool          |      str     |     int     |
        +----------+-------+-------+------------+-----+------------+----------+--------+------+--------------+----------+-------------------------+--------------+-------------+
        
        Arguments:
            args (tuple): The arguments passed into the load method by the transform method
                containing the dataframe. 
        
        """
        posts_df = args[0]

        # Creating connection to the database:
        con = sqlite3.connect(self.dbpath)

        # Writing the data to the database via pandas API:
        posts_df.to_sql(f"{self.subreddit_name}_posts", con, if_exists="append", index_label="id")

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


