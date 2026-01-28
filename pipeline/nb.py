#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# In[2]:


BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
FILE_NAME = "yellow_tripdata_2021-01.csv.gz"
CSV_URL = BASE_URL + FILE_NAME


# In[3]:


df_sample = pd.read_csv(CSV_URL, nrows=100)
df_sample.head()


# In[4]:


df_sample.dtypes
df_sample.shape


# In[5]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# In[6]:


engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")


# In[7]:


df_sample = pd.read_csv(
    CSV_URL,
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)

df_sample.head(0).to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="replace"
)

print("Table created")


# In[8]:


df_iter = pd.read_csv(
    CSV_URL,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100_000
)


# In[9]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )


# In[ ]:




