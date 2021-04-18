#!/usr/bin/env python
# coding: utf-8


import sqlite3
import pandas as pd
import json

business_path = 'yelp_academic_dataset_business.json/yelp_academic_dataset_business.json'
reviews_path = 'yelp_academic_dataset_review.json'
user_path = 'yelp_academic_dataset_user.json'

con = sqlite3.connect("yelp.db")


# generates string for manual sql create table statement

def make_sql_createtable(df, table_name, foreign_keys=None):
    ''' 
    df: pandas dataframe
    table_name: str, name of sql table
    foreign_keys:list of tuples consisting of arguments to FOREIGN KEY and REFERENCES to sql create table statement
    ex. FOREIGN_KEY <business_id> REFERENCES <business(business_id)> 
    
    returns create sql string ex. CREATE TABLE business .. (<colnames>)
    '''

    sql_datatypes = {'object': 'TEXT', 'float64': 'REAL', 'int64': 'INTEGER'}

    columns = []

    for key, val in df.dtypes.to_dict().items():
        columns.append(f'{key} {sql_datatypes[val.name]}')

        # make the first entry the primary id
    columns[0] = columns[0] + ' NOT NULL PRIMARY KEY'

    # add foreign keys
    if foreign_keys is not None:
        for key in foreign_keys:
            columns.append(f'FOREIGN KEY ({key[0]}) REFERENCES {key[1]}')

    column_string = ', '.join(columns)

    return f'CREATE TABLE {table_name} ({column_string})'


# # Part 1: Business Table Set-up

# use pandas read_json method to parse json file into dataframe
business = pd.read_json(business_path, lines=True)
business.head(2)

# subset non-nested columns from yelp's business dataset
business_table = business.iloc[:, :-3]

# make extra column for the business attribute foreign key
business_table['business_attribute_id'] = business_table['business_id']

# ### Load business Pandas Dataframe into the DB file
business_table.to_sql('business', con=con, if_exists='replace', index=False)

# Part 2: Business Attribute Table Set-Up


business_attributes_df = business[['business_id', 'attributes']].dropna()
business_attributes_df = business_attributes_df.to_json(orient='records')
business_attribute_df = pd.json_normalize(json.loads(business_attributes_df), meta=['business_id'])

### transformations to replace column names, remove unicode strings, remove unwanted columns


# if column name has the string 'attribute', remove it
business_attribute_df.columns = [i.split('.')[1] if 'attribute' in i else i for i in business_attribute_df.columns]

# remove unicode string encodings
business_attribute_df = business_attribute_df.applymap(lambda x: x.replace("u'", "'") if isinstance(x, str) else x)

### Load business attribute pandas dataframe to DB file

business_attribute_df.to_sql('business_attribute', con=con, if_exists='replace', index=False)

### Check if the business attribute table is indeed loaded the local db
pd.read_sql(''' SELECT * from business_attribute ''', con=con).head(3)

### Part 3: Reviews Table Set-Up


with open(reviews_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 1:
            data = line
        else:
            break

reviews_df = pd.DataFrame(json.loads(data), index=[])

reviews_create_sql = make_sql_createtable(reviews_df, 'reviews')
con.execute(reviews_create_sql)


def add_row(conn, tablename, rec):
    keys = ','.join(rec.keys())
    placeholders = ','.join(list('?' * len(rec)))
    values = tuple(rec.values())
    conn.execute('INSERT INTO ' + tablename + ' (' + keys + ') VALUES (' + placeholders + ')', values)



with open(reviews_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        data = json.loads(line)
        add_row(con, 'reviews', data)

con.commit()

# Part 4: User Table Set Up


with open(user_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i < 1:
            data = line
        else:
            break

users_df = pd.DataFrame(json.loads(data), index=[])

with con:
    users_create_sql = make_sql_createtable(users_df, 'users')
    con.execute(users_create_sql)

with open(user_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        data = json.loads(line)
        add_row(con, 'users', data)

con.commit()
con.close()

