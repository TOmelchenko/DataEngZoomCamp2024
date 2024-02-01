# Data Engineering Zoomcamp 2023 Week 2: Workflow Orchestration


The workflow is done by Mage (check out our docs [here](https://docs.mage.ai/introduction/overview))


## Configuring Mage

clone repo: https://github.com/mage-ai/mage-zoomcamp

go to the dir:
```
(base) tetianaomelchenko@Tetianas-Air ~ % cd /Users/tetianaomelchenko/Documents/GitHub/mage-zoomcamp 
(base) tetianaomelchenko@Tetianas-Air mage-zoomcamp % ls
Dockerfile		dev.env			requirements.txt
README.md		docker-compose.yml
```
Rename `dev.env` to simply `.env`— this will _ensure_ the file is not committed to Git by accident, since it _will_ contain credentials in the future.

```
(base) tetianaomelchenko@Tetianas-Air mage-zoomcamp % cp dev.env .env
```
build docker container:
```
(base) tetianaomelchenko@Tetianas-Air mage-zoomcamp % docker compose build
```
```
[+] Building 67.3s (9/9) FINISHED                                                                                                                             docker:desktop-linux
 => [magic internal] load build definition from Dockerfile                                                                                                                    0.0s
 => => transferring dockerfile: 359B                                                                                                                                          0.0s
 => [magic internal] load .dockerignore                                                                                                                                       0.0s
 => => transferring context: 2B                                                                                                                                               0.0s
 => [magic internal] load metadata for docker.io/mageai/mageai:latest                                                                                                         1.8s
 => [magic auth] mageai/mageai:pull token for registry-1.docker.io                                                                                                            0.0s
 => [magic internal] load build context                                                                                                                                       0.0s
 => => transferring context: 37B                                                                                                                                              0.0s
 => [magic 1/3] FROM docker.io/mageai/mageai:latest@sha256:ee285b5d85a2fb637d823e8eb91b9bd5a7e23823033ee6b087fad04abd124bed                                                  63.4s
 => => resolve docker.io/mageai/mageai:latest@sha256:ee285b5d85a2fb637d823e8eb91b9bd5a7e23823033ee6b087fad04abd124bed                                                         0.0s
 => => sha256:ee285b5d85a2fb637d823e8eb91b9bd5a7e23823033ee6b087fad04abd124bed 685B / 685B                                                                                    0.0s
 => => sha256:5665c1f9a9e17acd68ae05b2839df402eac34afdd095f8c115f09886d757840c 49.59MB / 49.59MB                                                                              8.9s
 => => sha256:f419b1a62fc83850ab3cb43274970bb20a18ae6e674535478a48f5bee11559b6 23.58MB / 23.58MB                                                                              5.1s
 => => sha256:76b4f1810f998c1f1580e2404b2e7fed8e264902d898bbe531443ea9789b7641 63.99MB / 63.99MB                                                                              3.3s
 => => sha256:0b65888bdf20e8468cc7721e69ff7300308ba97764fbbce9f149d3f9dab89701 3.47kB / 3.47kB                                                                                0.0s
 => => sha256:ae2f4a31d802938edb3efa3fff2af600bd87da5d92477b8093ef6f53171f1657 13.82kB / 13.82kB                                                                              0.0s
 => => sha256:1c176cbf649709b5d8a03720a6c53e18e33ad50feef33abe83c5ae95c5aabdb2 202.50MB / 202.50MB                                                                           22.1s
 => => sha256:ba0d9396537e9f0e9dfcfdbc88e19bf081ba7c18180e6db53fa370789e309f4d 6.47MB / 6.47MB                                                                                6.3s
 => => sha256:b29a44472c6e0f98f8ab448be77522d604a7b706c21b519e4a881471793caf40 16.84MB / 16.84MB                                                                              9.2s
 => => sha256:1810b7303c760a99755f9b863e266f0149066a9a753281509617d811d27857f4 246B / 246B                                                                                    9.1s
 => => extracting sha256:5665c1f9a9e17acd68ae05b2839df402eac34afdd095f8c115f09886d757840c                                                                                     1.3s
 => => sha256:14882fe1c6a8100768c780274d33ca1d7f95563dd22f96a45843491738af4b54 3.08MB / 3.08MB                                                                                9.9s
 => => sha256:c343c25e9b86acbdcace8539d46460abf75d46c974aada7c730ae41b27ae52b8 58.75MB / 58.75MB                                                                             18.9s
 => => sha256:9e32e0ea36d9e2668834b0acc5c06818dbe74ee257ba72b76f97c8b561c70601 2.84MB / 2.84MB                                                                               11.7s
 => => extracting sha256:f419b1a62fc83850ab3cb43274970bb20a18ae6e674535478a48f5bee11559b6                                                                                     0.3s
 => => extracting sha256:76b4f1810f998c1f1580e2404b2e7fed8e264902d898bbe531443ea9789b7641                                                                                     1.5s
 => => sha256:417ec439e6e5f27b58b42dae0c16b3733b8258422b6886072cdf49ade0e0edfd 124.01MB / 124.01MB                                                                           34.8s
 => => sha256:a9d1291a76690f6abc904e837cf22fdca7825f874705c8e7f388a26a54b625f2 361.84MB / 361.84MB                                                                           43.6s
 => => extracting sha256:1c176cbf649709b5d8a03720a6c53e18e33ad50feef33abe83c5ae95c5aabdb2                                                                                     4.1s
 => => sha256:cbe4e7fa75c1ef1d2157f73662885ee9cf4a80f064869f37f779cdff7ce723f9 408B / 408B                                                                                   22.4s
 => => sha256:94afca2d5f7cc85b82afcfce14e86039f882164390adcc4143cce2691b499d01 200.94MB / 200.94MB                                                                           37.5s
 => => extracting sha256:ba0d9396537e9f0e9dfcfdbc88e19bf081ba7c18180e6db53fa370789e309f4d                                                                                     0.2s
 => => extracting sha256:b29a44472c6e0f98f8ab448be77522d604a7b706c21b519e4a881471793caf40                                                                                     0.3s
 => => extracting sha256:1810b7303c760a99755f9b863e266f0149066a9a753281509617d811d27857f4                                                                                     0.0s
 => => extracting sha256:14882fe1c6a8100768c780274d33ca1d7f95563dd22f96a45843491738af4b54                                                                                     0.1s
 => => extracting sha256:c343c25e9b86acbdcace8539d46460abf75d46c974aada7c730ae41b27ae52b8                                                                                     0.7s
 => => extracting sha256:9e32e0ea36d9e2668834b0acc5c06818dbe74ee257ba72b76f97c8b561c70601                                                                                     0.0s
 => => extracting sha256:417ec439e6e5f27b58b42dae0c16b3733b8258422b6886072cdf49ade0e0edfd                                                                                     8.6s
 => => sha256:19f5950c59aebf8c2f4b1d1286a379e702815428e1aa0790da98edade017b556 1.05kB / 1.05kB                                                                               35.4s
 => => sha256:968e045fecdfa7291b4e18d990f1513fb1c38edb63f2966756fe9bbb936064cb 110B / 110B                                                                                   35.7s
 => => extracting sha256:a9d1291a76690f6abc904e837cf22fdca7825f874705c8e7f388a26a54b625f2                                                                                     9.9s
 => => extracting sha256:cbe4e7fa75c1ef1d2157f73662885ee9cf4a80f064869f37f779cdff7ce723f9                                                                                     0.0s
 => => extracting sha256:94afca2d5f7cc85b82afcfce14e86039f882164390adcc4143cce2691b499d01                                                                                     9.0s
 => => extracting sha256:19f5950c59aebf8c2f4b1d1286a379e702815428e1aa0790da98edade017b556                                                                                     0.0s
 => => extracting sha256:968e045fecdfa7291b4e18d990f1513fb1c38edb63f2966756fe9bbb936064cb                                                                                     0.0s
 => [magic 2/3] COPY requirements.txt /home/src/requirements.txt                                                                                                              0.6s
 => [magic 3/3] RUN pip3 install -r /home/src/requirements.txt                                                                                                                1.5s
 => [magic] exporting to image                                                                                                                                                0.0s
 => => exporting layers                                                                                                                                                       0.0s
 => => writing image sha256:99c42ac1045bc1046f910f81bd6f36b6931a008ca06acac474bdb76cee827066                                                                                  0.0s
 => => naming to docker.io/mageai/mageai:latest                                                                                                                               0.0s
```

to pull the latest mage version use 
```
(base) tetianaomelchenko@Tetianas-Air mage-zoomcamp % docker pull mageai/mageai:latest
```

run docker container (with mage + Postgres)

```
(base) tetianaomelchenko@Tetianas-Air mage-zoomcamp % docker compose up
```

Ther mage can be accessseble thru:http://localhost:6789/

the mail [screen](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline.png)


## ETL: API to Postgres
### test connection to Postgres

we'll build a simple ETL pipeline that loads data from an API into a Postgres database. Our database will be built using Docker— it will be running locally, but it's the same as if it were running in the cloud.


Update ```io_config.yaml``` to add the section of creating dev profiler section at very end:
```
  dev:
    POSTGRES_CONNECT_TIMEOUT: 10
    POSTGRES_DBNAME: "{{ env_var('POSTGRES_DBNAME') }}"
    POSTGRES_SCHEMA: "{{ env_var('POSTGRES_SCHEMA') }}" # Optional
    POSTGRES_USER: "{{ env_var('POSTGRES_USER') }}"
    POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
    POSTGRES_HOST: "{{ env_var('POSTGRES_HOST') }}"
    POSTGRES_PORT: "{{ env_var('POSTGRES_PORT') }}"
```

In mage 
edit the pipeline name to test_config

[img](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_config_1.png) 

create a sql data loader test_postgres to test connection to Postgres

[img](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_config_2.png)

in loader block set: 
- Connection - Postgres
- profile - dev
- run SELECT 1

[img](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_config_test_pg_connection.png)

### api to Postgres

In mage 
- create a new pipeline ```api_to_postgres```
- create python data loader ```load_api_data``` - the template appears - update template like in file below:
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
    """
    Template for loading data from API
    """
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
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
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    return pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtype, parse_dates=parse_dates)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```
- create python data transformer ```transform_taxi_data```- the template appears - update template like in file below:
```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
 
    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")

    return data[data['passenger_count'] > 0]


@test
def test_output(output, *args):
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'

```
- create python data exporter ``taxi_data_to_postgres```- the template appears - update template like in file below:
```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'ny_taxi'  # Specify the name of the schema to export data to
    table_name = 'yellow_taxi_data'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

```
- create a sql data loader ```load_taxi_data``` to check loaded data
- run ```select * from ny_taxi.yellow_taxi_data limit 10```

### configuring GCP

- create a new bucket in GCP Storage: ```mage_dezoomcamp2024_1```
- create a new service account: ```mage-dezoomcamp2024@dezc2024-taxi-data.iam.gserviceaccount.com``` with Owner permission
- create json Key for this service account and copy it into mage project folder - it will be automatically picked up because in docker compose file has already set up the volume. 
```
    volumes:
      - .:/home/src/
```
It means that all file from directory will be copied into project.

### Writing ETL pipeline: API to GCS
- create a new pipeline ```api_to_gcp```
- use already created blocks: ```load_api_data``` and ```transform_taxi_data```
- connect 2 blocks like in [img](/Users/tetianaomelchenko/Documents/GitHub/DataEngZoomCamp2024/img/mage_pipeline_1.png)
- create python data exporter ```taxi_data_to_gsc_parquet```
- in data exporter update:
```
    bucket_name = 'mage_dezoomcamp2024_1'
    object_key = 'nyc_taxi_data.parquet'
```
- run the pipeline - the ```nyc_taxi_data.parquet``` should appears in gcs
[img](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_2.png)

Not to load a big file into gcs we can divide it (make partitions). For this:
- create a new python(generic) data exporter ```taxi_data_to_gsc_partitioned_parquet``` 
- make a connection like [this](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_3.png))
- update ```taxi_data_to_gsc_partitioned_parquet``` like this:
```
import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dezc2024-taxi-data-3a8de483444e.json"

bucket_name = 'mage_dezoomcamp2024_1'
project_id = 'dezc2024-taxi-data'

table_name = 'nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['tpep_pickup_date'],
        filesystem=gcs
    )

```
- run the block - the partitioned data should appear in gcs: [gcs](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/mage_pipeline_4.png)




















