#  Yelp Academic Dataset  Analysis

The proposed final project will analyze Yelp's academic dataset (6GB) using data transformation concepts used in class with respect to relational databases. In particular the project will contain 4 main sections:

 1. Parsing large JSON files and DB transfer.
 2. EDA on Yelp Business and User data using SQL DML syntax.
 3. NLP on Yelp user reviews.
 4. Alternative Processing using ORM via SQL Alchemy



# Files and Project Data Schema

The dataset was provided by [Yelp through Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json) and the following data dumps were used for this analysis:

1. Yelp user data set (3.43 GB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_user.json
2. Yelp business data set(118.62 MB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json
3. Yelp review data set (6.46 GB) - https://www.kaggle.com/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_review.json

The files are joined and analyzed using the following relational schema below: 

![Project Relational DB Schema](https://i.ibb.co/KzdMNLB/schema.png)
