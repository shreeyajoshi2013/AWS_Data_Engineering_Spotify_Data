﻿# AWS Data Engineering Project - Spotify data analysis

This project aims to create a data pipeline that will enable the top-performing song playlist on Spotify to be accessed and utilised for data analysis.
It seeks to effectively set up a data pipeline that takes data straight from an API and transforms it into something that can be analysed and presented in a query-friendly manner. To import, extract, and process the data and make it accessible for analysis, the data pipeline makes use of AWS. 

Data source:
Portal of Spotify APIs -  [Spotify for Developers](https://developer.spotify.com/)

![My Image](DataFlowDiagram.jpg) 


### Highlights

* Data extraction via API
* Data cleansing
* Data transformation
* Data crawling 
* Data quering and analysis

### Main activities
* Integrating with Spotify API and extracting the data
* Using AWS S3 to store the files
* Deploying codes on AWS Lambda for Data Extraction and Transformation
* Adding trigger to schedule and run the function automatically
* Building Data Catalogs of data files using AWS Glue
* Analysing the tables using SQL in Athena


### AWS services used:

* Extraction:
    * Cloudwatch Trigger
    * Lambda Function
    * S3
* Transformation and loading:
    * Lambda Trigger 
    * Lambda Function
    * S3
* Data Catalogging
    * S3
    * Glue Crawlers
* Data Analysis
    * Athena Query Editor
* Other services
    * IAM


### File Formats Handled:

* JSON
* Pandas dataframe
* CSV

### Other Tools

* Miro board - Used for Data Pipeline Diagram 

### References

* https://datawithdarshil.com/
* https://aws.amazon.com/
