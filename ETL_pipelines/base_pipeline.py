"Script containing the Base framework for the Pipeline API"

# Importing External Pipeline Library:
import bonobo

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
