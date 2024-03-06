# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/workshop_2_5.png)

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.
```
CREATE MATERIALIZED VIEW zones_time AS
  SELECT z1.Zone AS dropoff_zone,
         z2.Zone AS pickup_zone,
         MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
         MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
         AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time
  FROM trip_data t
  JOIN taxi_zone as z1
  ON t.DOLocationID = z1.location_id
  JOIN taxi_zone as z2
  ON t.PULocationID = z2.location_id
  GROUP BY z1.Zone, z2.Zone;
```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/workshop_2_1.png)


From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.
I'm not sure that I understand what means anomalies. It could be treated like this but, anyway, it depends of number of trips.
```
CREATE MATERIALIZED VIEW anomalies AS
  SELECT dropoff_zone,
         pickup_zone,
         max_trip_time,
         min_trip_time,
         avg_trip_time
  FROM zones_time 
  WHERE (max_trip_time+ min_trip_time)/2 > avg_trip_time;
```


Options:
1. Yorkville East, Steinway - this is the answer
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/workshop_2_2.png)

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.
```
CREATE MATERIALIZED VIEW zones_time_count AS
  SELECT z1.Zone AS dropoff_zone,
         z2.Zone AS pickup_zone,
         MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
         MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
         AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
         COUNT(1) AS trips_number
  FROM trip_data t
  JOIN taxi_zone as z1
  ON t.DOLocationID = z1.location_id
  JOIN taxi_zone as z2
  ON t.PULocationID = z2.location_id
  GROUP BY z1.Zone, z2.Zone;
```

Options:
1. 5
2. 3
3. 10
4. 1 - this is the answer

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/workshop_2_3.png)

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 12:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.
```
CREATE MATERIALIZED VIEW busiest_zones AS
    WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, count(1) AS number_of_pickups
    FROM t, trip_data 
    JOIN taxi_zone
    ON trip_data.PULocationID = taxi_zone.location_id
    WHERE trip_data.tpep_pickup_datetime > t.latest_pickup_time - interval '17 hours'
	GROUP BY taxi_zone.Zone;
```
Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport - this is the answer
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/workshop_2_4.png)
