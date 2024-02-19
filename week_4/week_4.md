# Week 4: Analytics Engineering

## Prerequisites
1. I uploaded with ``` web_to_gcs.py``` the following datasets ingested from the course Datasets list:
- Yellow taxi data - Years 2019 and 2020
- Green taxi data - Years 2019 and 2020
- fhv data - Year 2019 and 2020-01.



``` web_to_gcs.py```  file:
```
import io
import os
import requests
import pandas as pd
from google.cloud import storage
import gzip

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key

$ export GOOGLE_APPLICATION_CREDENTIALS=“/Users/tetianaomelchenko/opt/bigquery/dezc2024-taxi-data-df0f32af6891.json”
$ gcloud auth application-default login
$ gcloud config set project dezc2024-taxi-data ###### dezc2024-taxi-data - this is project id 

If there is an error that the file with credentials is not found the credentials can be add to the script

3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/greengreen/green_tripdata_2019-01.csv.gz
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

#  add credentials to the script
credential_path = "/Users/tetianaomelchenko/opt/gcp/dezc2024-taxi-data-d5efc3c82183.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dezc2024_data_lake_dezc2024-taxi-data") ## here put your bucket


def upload_to_gcs(bucket, object_name, local_file):
    
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        print (request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")
        
        # read it back into a parquet file
        #with gzip.open(file_name, 'rb') as fio:
        #    df = pd.read_csv(fio)

        df = pd.read_csv(request_url, compression='gzip')
        file_name = file_name.replace('.csv.gz', '.parquet')
        
        if service == "yellow":
             """Fix dtype issues"""
             df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
             df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        if service == "green":
            """Fix dtype issues"""
            df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
            df["trip_type"] = df["trip_type"].astype('Int64')

        if service == "yellow" or service == "green":
            df["VendorID"] = df["VendorID"].astype('Int64')
            df["RatecodeID"] = df["RatecodeID"].astype('Int64')
            df["PULocationID"] = df["PULocationID"].astype('Int64')
            df["DOLocationID"] = df["DOLocationID"].astype('Int64')
            df["passenger_count"] = df["passenger_count"].astype('Int64')
            df["payment_type"] = df["payment_type"].astype('Int64')

        if service == "fhv":
            """Rename columns"""
            df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
            df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
            df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

            """Fix dtype issues"""
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

            # See https://pandas.pydata.org/docs/user_guide/integer_na.html
            df["PUlocationID"] = df["PUlocationID"].astype('Int64')
            df["DOlocationID"] = df["DOlocationID"].astype('Int64')

        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")



#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
#web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')
web_to_gcs('2020', 'fhv')

```


2. Created three tables for each dataset in BigQuery like this:

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_3.png)

## Setting dbt with BigQuery
1. Create dbt account.
2. Create an empty repository in GitHub - ```taxi_rides_ny``` 
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_1.png)
3. In GCP create a service account with BigQuery Admin permissions to connect with dbt
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_2.png)
4. For this service account the JSON key has been created, downloaded and then uploaded into dbt during the new project creation
5. In dbt create dbt project with BigQuery - ```taxi_rides_ny ``` 
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_5.png)
Detailed info how to set up dbt project in the ![link]https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md
6. Add connection to BigQuery - upload JSON key
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_6.png)
7. for dbt project setup a repository - add newly created repository in GitHub - ```taxi_rides_ny``` 
8. Go to develop tab - the empty folder ```taxi_rides_ny``` should appear.
9. Press Initiate a project - the all necessary folders and files for the new dbt project should appear.
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_4.png)
10. Create brunch to edit default files. If you are on the main branch, you will need to create a new branch before being able to edit your project. dbt cloud doesn’t let you edit code on the main branch to encourage good branching hygiene.
11. Edit ```dbt_project.yml```
```

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'taxi_rides_ny'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'default'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In dbt, the default materialization for a model is a view. This means, when you run 
# dbt run or dbt build, all of your models will be built as a view in your data platform. 
# The configuration below will override this setting for models in the example folder to 
# instead be materialized as tables. Any models you add to the root of the models folder will 
# continue to be built as views. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.

models:
  taxi_rides_ny:
    # Applies to all files under models/example/

```

12. In Bigquery add 3 datasets: dbt_tetiana_omelchenko, production, staging
The name of data set for dbt models should be the same as was set for dbt project
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_5.png)

## Models
13. Create  source file ```models/example/schema.yml```
```
version: 2

sources:
  - name: staging
    database: dezc2024-taxi-data
    schema: trips_data_all

    tables:
      - name: green_trips_data
      - name: yellow_trips_data
      - name: fhv_trips_data
```
14. Create model file ```models/staging/stg_green_trips_data.sql```
```
{{ config(materialized="view") }}

select * from {{ source("staging","green_trips_data")}}
limit 100
```
15. Run model - the view should appear in BigQuery under the folder/dataset - dbt_tetiana_omelchenko
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_7.png)
16. For consistency modify the query models/staging/stg_green_tripdata.sql by indicating the columns like this:
```
{{ config(materialized="view") }}

select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge    
from {{ source('staging', 'green_trips_data')}}
limit 100
```
17. when run the updated model ```dbt run --select stg_green_trips_data``` get this result:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_8.png)

## Macros
18. In the folder ```macros\``` create a macro-file ```get_payment_type_description.sql```
```
{#
    This macro returns the description of the payment_type
#}

{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

{%- endmacro %}
```
19. add macro to the model file:
```
{{ config(materialized="view") }}

select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge    
from {{ source('staging', 'green_trips_data')}}
limit 100
```
20. run model ```dbt run --select stg_green_trips_data```
21. compiled code can be found ```You can also see the compiled code under target > compiled > taxi_rides_ny > models > staging > stg_green_trips_data.sql.```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_9.png)
## Packages
22. create package -> in main folder create ```packages.yml```:
```
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```
23. to install packages that defined in ```packages.yml``` run command ``` dbt deps```. We should see logs and a lot of folders and files created under dbt_packages/dbt_utils.
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_11.png)
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_10.png)
24. create a surrogate key in our model using ```dbt_utils``` package:
```
{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,

...

from {{ source('staging', 'green_trips_data')}}
limit 100
```
and run dbt model with command ```dbt run --select stg_green_trips_data```
Here the compiled code:
```
select
    -- identifiers
    to_hex(md5(cast(coalesce(cast(vendorid as 
    string
), '') || '-' || coalesce(cast(lpep_pickup_datetime as 
    string
), '') as 
    string
))) as tripid,

...

from `dezc2024-taxi-data`.`trips_data_all`.`green_trips_data`
limit 100
```
## Variables
Global variable we define under dbt_project.yml.

```
vars:
  payment_type_values: [1, 2, 3, 4, 5, 6]
```
25. add variable used in CLI to our model that can define select statement with or without limit 1000. :
```
from {{ source('staging', 'green_trips_data') }}
where vendorid is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
26. run ```dbt run --select stg_trips_green_tripdata --vars 'is_test_run: false'" ``` load all data without limit
27. build model  ```stg_yellow_trips_data.sql```for yellow trips data:
```
{{ config(materialized='view') }}

select
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from {{ source('staging', 'yellow_trips_data') }}
where vendorid is not null

-- dbt run --select stg_yellow_tripdata --vars 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
```
and run both models: ```dbt run --vars 'is_test_run: false' ```

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_12.png)
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_13.png)


## Seeds
28.The ```dbt seed``` command will load csv files located in the seed-paths directory of your dbt project into your data warehouse.

In our dbt cloud, create seeds folder, create the file ```seeds/taxi_zone_lookup.csv``` and paste in it the content of that ![csv-file](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/seeds/taxi_zone_lookup.csv).

After, run the command dbt seed to create table taxi_zone_loopup in BigQuery. We should have 265 lines.

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_14.png)

29. We need to specify the data types of the csv file in ```dbt_project.yml```:
```
seeds:
    taxi_rides_ny:
        taxi_zone_lookup:
            +column_types:
                locationid: numeric
```
30. modify data in csv file (for example, change 1,"EWR","Newark Airport","EWR" for 1,"NEWR","Newark Airport","EWR"). To get this data changed in BigQuery run the following command:
```
dbt seed --full-refresh
```
It will drop the existing table and create a new one.
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_15.png)

31. create the file ``models/core/dim_zones.sql```:
```
{{ config(materialized='table') }}


select
    locationid,
    borough,
    zone,
    replace(service_zone,'Boro','Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
```

32. create the fact model ```models/core/fact_trips.sql```that union all data:
```
{{ config(materialized='table') }}

with green_data as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_trips_data') }}
),

yellow_data as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_trips_data') }}
),

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    trips_unioned.tripid,
    trips_unioned.vendorid,
    trips_unioned.service_type,
    trips_unioned.ratecodeid,
    trips_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count,
    trips_unioned.trip_distance,
    trips_unioned.trip_type,
    trips_unioned.fare_amount,
    trips_unioned.extra,
    trips_unioned.mta_tax,
    trips_unioned.tip_amount,
    trips_unioned.tolls_amount,
    trips_unioned.ehail_fee,
    trips_unioned.improvement_surcharge,
    trips_unioned.total_amount,
    trips_unioned.payment_type,
    trips_unioned.payment_type_description,
    trips_unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```
The command ```dbt run``` will run all models but not seed. To run all models and seed use ```dbt build```. Also the command ```dbt build --select +fact_trips.sql`` will run model ```act_trips.sql``` with ALL dependencies

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_15.png)

## dbt deployment
There are 2 types of jobs:
- scheduled job
- CI job, that is been triggered by PR. Currently it's not possible to do this in dbt development plan:
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_18.png)

I created scheduled job ```Update taxi_rides_ny```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_19.png)
I used ```dbt build``` because I updated staging scripts.
```
{% if var('is_test_run', default=false) %}
```
p.s. not to run all staging models separately with ```dbt run --vars 'is_test_run: false'``` I updated this in staging scripts to get all data.

I committed all changes in my_first_branch, created PR, and merged to main.
Then I launched my job manually --> all database objects have been created in BigQuery in schema ```	production```

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_20.png)

## Looker Studio (Google Data Studio)

To create a dashboard I made a combined dataset ```models/core/fact_trips_combined.sql``` and deployed it to production. This dataset was used as a source for my dashboard.
```


with green_data as (
    select 
        tripid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        'Green' as service_type
    from `dezc2024-taxi-data`.`dbt_tetiana_omelchenko`.`stg_green_trips_data`
    where extract(year from pickup_datetime) = 2019
),

yellow_data as (
    select 
        tripid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        'Yellow' as service_type
    from `dezc2024-taxi-data`.`dbt_tetiana_omelchenko`.`stg_yellow_trips_data`
    where extract(year from pickup_datetime) = 2019
),


fhv_data as (
    select 
        tripid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        'FHV' as service_type
    from `dezc2024-taxi-data`.`dbt_tetiana_omelchenko`.`stg_fhv_trips_data`
),

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
    union all    
    select * from fhv_data
),

dim_zones as (
    select * from `dezc2024-taxi-data`.`dbt_tetiana_omelchenko`.`dim_zones`
    where borough != 'Unknown'
)
select
    trips_unioned.tripid,
    trips_unioned.service_type,
    trips_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```

https://lookerstudio.google.com/s/sQcbGoKUzdo
