# Importing data management libraries:
import pandas as pd
import yfinance as yf
import sqlite3

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
    pass

class DJIACompositionWebAPIPipeline(DJIACompositionPipeline):
    pass

class SPTSXCompositionWebAPIPipeline(SPTSXCompositionPipeline):
    pass

class FTSECompositionWebAPIPipeline(FTSECompositionPipeline):
    pass

class SMICompositionWebAPIPipeline(SMICompositionPipeline):
    pass

class SPICompositionWebAPIPipeline(SPICompositionPipeline):
    pass