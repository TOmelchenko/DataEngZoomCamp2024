## Module 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres - only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 
- It's the same as running *dbt build*
- It applies a _limit 100_ to all of our models
- It applies a _limit 100_ only to our staging models - it's the correct answer
- Nothing

### Question 2: 

**What is the code that our CI job will run?**  

- The code that has been merged into the main branch
- The code that is behind the object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- The code from a development branch requesting a merge to main - it's the correct answer


### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.

File ```models/staging/stg_fhv_trips_data.sql```
```
{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as integer) as sr_flag,
    Affiliated_base_number as affiliated_base_number
      
from {{ source('staging', 'fhv_trips_data') }}
where extract(year from pickup_datetime) = 2019
and dispatching_base_num is not null

-- dbt build <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

```

Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations.
File ```models/core/fact_fhv_trips.sql```
```
{{ config(materialized='table') }}

with fhv_data as (
    select *,
        'FHV' as service_type
    from {{ ref('stg_fhv_trips_data') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_data.tripid,
    fhv_data.service_type,
    fhv_data.dispatching_base_num,
    fhv_data.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.sr_flag,
    fhv_data.affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
```
 
Run the dbt model without limits (is_test_run: false).

- 12998722
- 22998722 - it's the correct answer
- 32998722
- 42998722

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_17.png)

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- Yellow - it's the correct answer
- FHV and Green

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/dbt_21.png)

Link to Dashboard: https://lookerstudio.google.com/s/sQcbGoKUzdo





