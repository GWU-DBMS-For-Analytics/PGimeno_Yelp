#  Yelp Academic Dataset  Analysis
by Paul Gimeno - 4/20/21

This project will analyze Yelp's academic dataset (6GB) using data transformation concepts used in class with respect to relational databases. It will cover the following topic areas:

 1. Parsing large JSON files and loading python objects into permanent storage (relational db)
 2. EDA on Yelp Business and user data to answer basic questions using SQL DML syntax.
 3. Perform basic sentiment analysis by leveraging freemium API's available on the web on Yelp user reviews.
 4. Run the same analysis via an ORM using SQL Alchemy



# Files and Project Data Schema

The dataset was provided by [Yelp through Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json) and the following data dumps were used for this analysis:

1. Yelp user data set (3.43 GB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_user.json
2. Yelp business data set(118.62 MB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json
3. Yelp review data set (6.46 GB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_review.json

The files are joined and analyzed using the following relational schema below: 

![Project Schema](https://i.ibb.co/KzdMNLB/schema.png)


# Section 1: Loading JSON objects into SQLite tables 

The project notebook with the full code can be [found here:](https://github.com/GWU-DBMS-For-Analytics/PGimeno_Yelp/blob/ffc088871ec8acc0c7f9112c74cc4cdcb0e63657/1%20-%20Data%20Transformation%20and%20Loading.ipynb)

Through python's native JSON module and Pandas' json_normalize function, we can easily load json objects into a pandas dataframe, perform transformations on it and load them into a database table such as Sqlite.

The following snippet shows one approach (see notebook for full implementation)
 

    import json
    import sqlite3
    import pandas as pd
    with sqlite3.connect('yelp_project.db') as con:
	    #read json file and assign to dataframe
	    business = pd.read_json('business.json',line=True)
	    
	    #create a db table called business and insert/replace all columns/rows into the dataframe
	    
	    business.to_sql('business', con=con, if_exists='replace')

	
	    

  

# Section 2: Yelp Business EDA

The goal of this section is to answer a few basic questions using SQL syntax

**Question 1:** Which states have the most business listings, reviews and average stars?

**Question 2:** Which of the top states have the highest average reviews, business count and stars?

**Question 3:** In our chosen state, which city has the highest concentration of businesses and reviews?

**Question 4:** Within this city, which are the top 10 most reviewed establishments and what are some of their attributes?

**Question 5:** For the most popular restaurant in this city, who is a repeat user reviewer that is the most negative?

**Question 6:** For this most negative reviewer in portland, where else does he spread his negativity?


# Section 3: Basic NLP using Google's Text Analysis API

[Freemium API used in the project](https://rapidapi.com/insights-ml-insights-ml-default/api/google-text-analysis) 

Notebook and code can be found here:

**Question 7:** For this most negative reviewer in portland, are all his reviews negative in general? 


