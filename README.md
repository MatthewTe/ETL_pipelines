# Bonobo ETL Pipeline APIs
A repository containing the methods and scripts used to maintain various databases. The repo contains various APIs for setting up and executing small-scale ETL pipelines using the [Bonobo](https://github.com/python-bonobo) library.

The library was designed mainly for specific personal use but is generalizable. It was built with ease of use and simplicity at all costs as such it is very light weight. Use Airflow or something else if you have specific needs please.

This library was the result of a conversation with a friend that lead to me attempting to stand up a fully functional algorithmic trading and quant analysis environment without all of the traditional "bells and whistels" of a more heavy-weight environment. As such this library contains only the bare-bones functionality necessary:

- It was designed with the file system database sqlite in mind. No external Postgres or NOSQL connections.
- All ETL pipelines are built with the [bonobo](https://github.com/python-bonobo) library, making use of fairly basic scheduled Directed Asyclic Graphs to executre ETL functions.

As I was developing the ETL Pipeline API I also decided to write a VERY basic Query API to make feeding data stored in my sqlite database into other applications. This Query API shamelessly wraps the [Pandas](https://pandas.pydata.org/docs/) database query API, so very little creative credit can go to me for it. It is also developed and expaned upon in a strictly needs-based manner, with simplicity as its core philosophy. Use it (any of it really) at your own risk.

## The ETL Pipeline APIs that are currently available are:

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

**Social Media Data `ETL_pipelines/social_media_pipeline`**:
```
Reddit Data
-----------------------------------------------------------------------------------------------------------
Subreddit Daily Rising and Top Posts Content - reddit_submissions.RedditContentPipeline
```

## Bonobo Web API Pipelines
I did say that the Bonobo ETL Pipeline was generalizable and so as my needs evolved, so did the library. I developed a Django REST API service for my other applications, so I decided to write a sub-module for the Pipeline API that perform all of the ETL functions to a Web based REST API instead of a local sqlite database. This either takes the form of inheriting an existing sqlite Pipeline API and replacing the `load`
portion of the Bonobo graph, or in more complicated instances refactoring the whole Pipeline Object. Refactoring the entire project will take time and at the point that all of the APIs get converted from sqlite pipelines to REST web API pipelines then the README will be changed to incorporate that fact.

## The Web Pipeline APIs that are currently available are:
** Stock Data `ETL_pipelines/web_api_pipelines/stock_pipeline`
```
Composition Data
-----------------------------------------------------------------------------------------------------------
Wikipedia S&P 500 Index Composition - market_indicies.SPYCompositionPipeline
Wikipedia Dow Jones Industrial Average Index Composition - market_indicies.DJIACompositionPipeline
Wikipedia S&P/TSX Index Composition - market_indicies.SPTSXCompositionPipeline
Wikipedia FTSE 100 Index Composition - market_indicies.FTSECompositionPipeline
Wikipedia Swiss Market Index Composition - market_indicies.SMICompositionPipeline
Wikipedia Swiss Performance Index Composition - market_indicies.SPICompositionPipeline

``` 
**Social Media Data `ETL_pipelines/web_api_pipelines/social_media_pipeline`**:
```
Reddit Data
-----------------------------------------------------------------------------------------------------------
Subreddit Daily Rising and Top Posts Content - reddit_submissions.RedditContentPipeline
```