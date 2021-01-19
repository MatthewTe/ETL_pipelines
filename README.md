# Bonobo ETL Pipeline APIs
A repository containing the methods and scripts used to maintain various databases. The repo contains various APIs for setting up and executing small-scale ETL pipelines using the [Bonobo](https://github.com/python-bonobo) library.

The library was designed mainly for specific personal use but is generalizable. It was built with ease of use and simplicity at all costs as such it is very light weight. Use Airflow or something else if you have specific needs please.

This library was the result of a conversation with a friend that lead to me attempting to stand up a fully functional algorithmic trading and quant analysis environment without all of the traditional "bells and whistels" of a more heavy-weight environment. As such this library contains only the bare-bones functionality necessary:

- It was designed with the file system database sqlite in mind. No external Postgres or NOSQL connections.
- All ETL pipelines are built with the [bonobo](https://github.com/python-bonobo) library, making use of fairly basic scheduled Directed Asyclic Graphs to executre ETL functions.

## The ETL Pipelines that are currently avalible are:

**Stock Data `ETL_pipelines/stock_pipeline`**:
```
Pricing Data
-----------------------------------------------------------------------------------------------------------
OHLC Yahoo Finance data - OHLC.OHLCPipeline

Composition Data
-----------------------------------------------------------------------------------------------------------
Wikipedia S&P 500 Index Composition - market_indicies.SPYCompositionPipeline
Wikipedia Dow Jones Industrial Average Index Composition - market_indicies.DJIACompositionPipeline
Wikipedia S&P/TSX Index Composition - market_indicies.SPTSXCompositionPipeline
Wikipedia FTSE 100 Index Composition - market_indicies.FTSECompositionPipeline
Wikipedia Swiss Market Index Composition - market_indicies.SMICompositionPipeline
Wikipedia Swiss Performance Index Composition - market_indicies.SPICompositionPipeline

SEC Filings Data
-----------------------------------------------------------------------------------------------------------
SEC EDGAR Filings Content - sec_filings.EDGARFilingsPipeline
```
    