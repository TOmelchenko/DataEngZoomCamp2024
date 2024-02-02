- Create a new pipeline, call it green_taxi_etl

![start](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/homework_week2_pipeline.png)

![final](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_6.png)

- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months 10, 11, 12).
  - You can use the same datatypes and date parsing methods shown in the course.
  - BONUS: load the final three months using a for loop and pd.concat

The block ```load_green_taxi_data.py``` has been created:
```
import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
    url2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz'  
    url3 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    
    taxi_dtype = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float
                }       
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dfs = list()
    
    #url = list()
    for i in range(0, 3):
        url = path + 'green_tripdata_2020-1' + str(i) + '.csv.gz'
        
        ##print (url)

        df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtype, parse_dates=parse_dates)
        dfs.append(df)
    
    data=pd.concat(dfs, axis=0, ignore_index=True)

    ## Without loop
    ##df1 = pd.read_csv(url1, sep=",", compression="gzip", dtype=taxi_dtype, parse_dates=parse_dates)
    ##df2 = pd.read_csv(url2, sep=",", compression="gzip", dtype=taxi_dtype, parse_dates=parse_dates)
    ##df3 = pd.read_csv(url3, sep=",", compression="gzip", dtype=taxi_dtype, parse_dates=parse_dates)
    
    #data=pd.concat([df1,df2,df3], axis=0, ignore_index=True)

    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```
- Add a transformer block and perform the following:x
  - Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
  - Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
  - Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
- Add three assertions:
  - vendor_id is one of the existing values in the column (currently)
  - passenger_count is greater than 0
  - trip_distance is greater than 0

The block ```transform_green_taxi_data.py``` has been created:

```
import pandas as pd
from stringcase import pascalcase, snakecase

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    ##add date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    ##rename column
    df = pd.DataFrame(data)

    ## variant 1
    ## not fully matched because VendorID became vendor_i_d
    ##df.columns = [snakecase(col) for col in df.columns]

    ## variant 2
    ## not fully matched because VendorID became vendorid   
    ##df.columns = [col.lower() for col in df.columns]

    ## variant 3
    ## not elegant but it works
    df.rename(columns={'VendorID': 'vendor_id'}, inplace=True)
    df.rename(columns={'RatecodeID': 'ratecode_id'}, inplace=True)
    df.rename(columns={'PULocationID': 'pu_location_id'}, inplace=True)
    df.rename(columns={'DOLocationID': 'do_location_id'}, inplace=True)

    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")
    print(f"Preprocessing rows with zero trip distance: { data[['trip_distance']].isin([0]).sum() }")

    # check vendor_id values
    vendor_id_values = df['vendor_id'].unique().tolist()
    print(vendor_id_values)

    # null to 0
    data['vendor_id'].fillna(0, inplace=True)

    # check vendor_id values
    vendor_id_values1 = df['vendor_id'].unique().tolist()
    print(vendor_id_values1)

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)] #& (data['vendor_id'] > 0)]


@test
def test_output(output, *args) -> None:

    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with 0 trip distance'
    assert output['vendor_id'].isin([0]).sum() == 0, 'There are rides with empty vendor_id. Please check the data'
```    



- Using a Postgres data exporter (SQL or Python), write the dataset to a table called green_taxi in a schema mage. Replace the table if it already exists.
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_5.png)

- Write your data as Parquet files to a bucket in GCP, partioned by lpep_pickup_date. Use the pyarrow library!

![gcs](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_7.png)

- Schedule your pipeline to run daily at 5AM UTC.

![schedule](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_8.png)

# Question 1. Data Loading
Once the dataset is loaded, what's the shape of the data?

- 266,855 rows x 20 columns - this one is the answer
- 544,898 rows x 18 columns
- 544,898 rows x 20 columns
- 133,744 rows x 20 columns

![answer](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/homework_week2_q1.png)


# Question 2. Data Transformation
Upon filtering the dataset where the passenger count is equal to 0 or the trip distance is equal to zero, how many rows are left?

- 544,897 rows
- 266,855 rows
- 139,370 rows - this one is the answer
- 266,856 rows

![answer](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/homework_week2_q2.png)


# Question 3. Data Transformation
Which of the following creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date?

- data = data['lpep_pickup_datetime'].date
- data('lpep_pickup_date') = data['lpep_pickup_datetime'].date 
- data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date  - this one the answer
- data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()

# Question 4. Data Transformation
What are the existing values of VendorID in the dataset?

- 1, 2, or 3
- 1 or 2 - this one is the answer
- 1, 2, 3, 4
- 1

# Question 5. Data Transformation
How many columns need to be renamed to snake case?

- 3
- 6
- 2
- 4 - this one is the answer

# Question 6. Data Exporting
Once exported, how many partitions (folders) are present in Google Cloud?

- 96 - this one is the answer: 95 for data files + 1 parent directory
- 56
- 67
- 108


