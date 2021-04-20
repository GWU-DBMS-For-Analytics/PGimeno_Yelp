#  Yelp Academic Dataset  Analysis
by Paul Gimeno - 4/20/21

This project will analyze Yelp's academic dataset (9+GB) using data transformation concepts used in class with respect to relational databases. It will cover the following topic areas:

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

![enter image description here](https://i.ibb.co/fSB5DVZ/schema.png)


# Section 1: Loading JSON objects into SQLite tables 

The project notebook with the full code can be [found here:](https://github.com/GWU-DBMS-For-Analytics/PGimeno_Yelp/blob/ffc088871ec8acc0c7f9112c74cc4cdcb0e63657/1%20-%20Data%20Transformation%20and%20Loading.ipynb)

Through python's native JSON module and Pandas' json_normalize function, we can easily load json objects into a pandas dataframe, perform transformations on it and load them into a database table such as Sqlite.

The following snippet shows one approach (see notebook for full implementation). 
 

    import json
    import sqlite3
    import pandas as pd
    with sqlite3.connect('yelp_project.db') as con:
	    #read json file and assign to dataframe
	    business = pd.read_json('business.json',line=True)
	    
	    #create a db table called business and insert/replace all columns/rows into the dataframe
	    
	    business.to_sql('business', con=con, if_exists='replace')

	
	    

  

# Section 2: Yelp Business EDA

The goal of this section is to answer a few basic questions using SQL syntax. Using a helper function (as shown below) along with Pandas can produce clean intermediate results because we can work directly with DataFrames.

Full code implementation can be viewed [here](https://github.com/GWU-DBMS-For-Analytics/PGimeno_Yelp/blob/main/2%20-%20Yelp%20EDA%20and%20Sentiment%20Analysis.ipynb)
To display sql results in dataframes rather than tuples, i defined a helper function as shown below:

    
    import pandas as pd
    import sqlite3
    
    def run_query(sql):
  
	    with sqlite3.connect('yelp.db') as con:
	        try:
	            df = pd.read_sql(sql, con=con)
	            return df
	        except Exception as bad:
	            print(bad)

   
**Question 1:** **Which states have the most business listings, reviews and average stars?**

We can query states that have the most business listings and reviews. In this case, we are filtering for states with at least 10,000 businesses.

    [in] sql = '''
    SELECT distinct state, 
    AVG(review_count) average_reviews, 
    AVG(stars) average_stars, 
    count(business_id) number_of_businesses 
    FROM business 
    GROUP BY state
    HAVING count(business_id) > 10000
    order by 3 desc '''
    
    [in] run_query(sql)
    [out] 
   

 ![result dataframe 1](https://i.ibb.co/6bwKxj0/EDA-most-states.png)

Oregon appears to have the best average stars and among the top contenders for most business listings and so we'll arbitrarily choose the state of Oregon

**Question 2:** **In the state of OR, which city has the highest concentration of businesses and reviews?**

    [in] sql = '''
    SELECT distinct(city), 
    state, 
    count(business_id) num_businesses
    from business
    where state = 'OR'
    GROUP BY city
    ORDER BY 3 DESC
    LIMIT 1 '''
    [in] run_query(sql)
    [out]
![EDA-2-Portland](https://i.ibb.co/QJLxjRD/EDA-2-Portland.png)

The result returns the top city of Portland

**Question 3:** **Within this city, which are the top 10 most reviewed establishments and what are some of their attributes?**

    [in] sql = '''
    SELECT business_id, name, city, 
    state, review_count, 
    avg(stars) average_stars
    FROM business
    where state = 'OR' and city = 'Portland'
    GROUP BY business_id
    ORDER BY 5 DESC
    LIMIT 10'''
    
    [in] run_query(sql)
    [out]
    

![EDA-3](https://i.ibb.co/1nDSdrM/EDA-3-Top-In-Portland.png)

For these 10 establishments, let's determine their attributes by joining the business table with the business attributes table

    [in] sql = '''
    SELECT * FROM business_attribute ba
    INNER JOIN (SELECT business_id, name, review_count
    from business 
    where state = 'OR' and city = 'Portland'
    ORDER BY 3 DESC
    LIMIT 10) b on ba.business_id = b.business_id
    '''
    [in] run_query(sql)
    [out] 


Our result returns a dataframe with business characteristics. Here are a few features that were obtained as an example.

Best nights to visit among these establishments:
![enter image description here](https://i.ibb.co/jy0jxrD/EDA-best-nights.png)
Ambience among these establishments:
![enter image description here](https://i.ibb.co/KXk9D9b/EDA-business-ambience.png)
Type of meals served
![enter image description here](https://i.ibb.co/DDTnSzk/EDA-Good-For-Meal.png)
    

**Question 4:** For the most popular restaurant in this city, who is a repeat user reviewer that is the most negative?

    [in] sql = '''
    SELECT r.user_id, u.name username, r.business_id,
    COUNT(r.review_id) num_reviews, AVG(r.stars) avg_rating
    FROM reviews r
    INNER JOIN users u on r.user_id = u.user_id
    WHERE r.business_id = '4CxF8c3MB7VAdY8zFb2cZQ'
    GROUP BY r.user_id
    HAVING COUNT(r.review_id) > 1
    ORDER by 4 desc, 3 asc
    LIMIT 5'''
    
    [in] run_query(sql)
![enter image description here](https://i.ibb.co/dr65jM1/EDA-mostnegative-username.png)

As we can see, username David gave the most review and the most negative review (on average) from our result set

**Question 5:** For this most negative reviewer in portland,  does David spread his negativity elsewhere?

    [in] sql = '''
    SELECT avg(r.stars) avg_stars, count(r.review_id)
    total_review_count
    FROM reviews r
    WHERE r.user_id = 'Q2u4PQ5_aMBAQtyX19C1Iw'
    and r.business_id != '4CxF8c3MB7VAdY8zFb2cZQ'
    '''
    [in] run_query(sql)
    
![enter image description here](https://i.ibb.co/br1rTH9/EDA-David-Avg.png)

Based on his reviews elsewhere, it does not seem like David is the negative reviewer in general. Can we quantify his sentiment to see if his average stars correlate with his overall sentiment?

# Section 3: Basic NLP using Google's Text Analysis API

[Freemium API used in the project](https://rapidapi.com/insights-ml-insights-ml-default/api/google-text-analysis) 


**Question 7:** What is David's tone and sentiment in most of his reviews?

To answer this question and estimate David's overall sentiment, we can analyze all of his reviews using natural language processing. In particular, we can utilize publically accessible, pre-trained sentiment analyzer which can score David's overall sentiment from -1 (negative) to +1(positive) with 0 as neutral.

The sentiment function i used is shown below, it's a simple POST request which utilize python's native requests library

    def add_sentiment_score(text):
	    url = "https://google-text-analysis.p.rapidapi.com/AnalyzingSentiment"
	    
	    #remove quotes in the request string
	    text = text.replace('"',"")	    
	    
	    print(text)
	    payload = "{\r\"message\": \"" + text + "\"\r}"
	    headers = {
	        'content-type': "application/json",
	        'x-rapidapi-key': TEXT_API_KEY,
	        'x-rapidapi-host': "google-text-analysis.p.rapidapi.com"
	        }

	    response = requests.request("POST", url, data=payload, headers=headers)

	    resp = response.text
	    try:
	        score = json.loads(resp)['documentSentiment']['score']
	        print(score)
	        
	        #api throws exceptions when the 1 request per second rule is broken
	        time.sleep(5)
	        return score
	        	    
	    except Exception as e: 
	        print(e)


This function can be applied to a map function within Pandas dataframe as shown:

    df['score'] = df['text'].map(add_sentiment_score)

Partial results of this processing can be appended to a dataframe as shown below. As we can see, it does a really good approximation.

![enter image description here](https://i.ibb.co/jGbR1Dx/david-nlp-1.png)

When we look at David's mean sentiment score, he averages at .557 which is fairly positive. Therefore, we can conclude that he is not a habitual bad reviewer.
# Section 4: Alternate querying using SQL Alchemy

The motivation for exploring an ORM like SQL Alchemy is  as follows:

1. I am able to represent data within the database (rows, columns, records) into Python objects and model the data to align with another application.

2.  In the event that i need to change my persistent storage from Sqlite into another database such as a Mysql instance in an RDS instance, the underlying code does not need to change as the ORM database engine can handle different flavors of SQL syntax that represents the current data model. 

An ORM version of the analysis performed in the previous section can be viewed [here](https://github.com/GWU-DBMS-For-Analytics/PGimeno_Yelp/blob/main/3%20-%20Analysis%20using%20ORM.ipynb)

For example, to find all businesses with business listings above 10K, we can represent them as:

    query = (
		    session.query(business.c.state,func.count(business.c.business_id).label('num_businesses'))
		    .group_by(business.c.state)
		    .having(func.count(business.c.business_id) > 10000)
		    .order_by(func.count(business.c.business_id).desc())     
    )

results displayed in a dataframe:
![enter image description here](https://i.ibb.co/tPSZJmq/ORM-10states.png)

Instead of writing nested SQL queries, we can instead chain method calls and it often makes the query more readable.
