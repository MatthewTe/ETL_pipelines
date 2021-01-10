# Bonobo ETL Pipeline APIs
A repository containing the methods and scripts used to maintain various databases. The repo contains various APIs for setting up and executing small-scale ETL pipelines using the [Bonobo](https://github.com/python-bonobo) library.

The library was designed mainly for specific personal use but is generalizable. It was built with ease of use and simplicity at all costs as such it is very light weight. Use Airflow or something else if you have specific needs please.

## The ETL Pipelines that are currently avalible are:

**Stock Data `ETL_pipelines/stock_pipeline`**:
```
OHLC Yahoo Finance data - OHLC.OHLCPipeline
Wikipedia S&P 500 Index Composition - market_indicies.SPYCompositionPipeline
Wikipedia Dow Jones Industrial Average Index Composition - market_indicies.DJIACompositionPipeline
Wikipedia S&P/TSX Index Composition - market_indicies.SPTSXCompositionPipeline
Wikipedia FTSE 100 Index Composition - market_indicies.FTSECompositionPipeline
Wikipedia Swiss Market Index Composition - market_indicies.SMICompositionPipeline
```
