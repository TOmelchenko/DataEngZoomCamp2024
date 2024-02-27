# Week 5. Batch processing

[link to MacOS installation](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/macos.md)

## Instal Java
Install Brew and Java, if not already done.
```
# Install Homebrew.
xcode-select –install /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
# Install Java.
brew install java
```
Find where Java is installed.

- Running brew list shows a list of all your installed Homebrew packages. If you run brew list, you should see openjdk@17.
- Running which brew indicates where brew is installed. Brew is installed in ```/opt/homebrew/bin/brew.```
- In my case Java has been installed to ```/opt/homebrew/Cellar/openjdk/21.0.2```
So, I add these instructions in my ~/.zshrc.
```
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@17/17.0.9"
export PATH="$JAVA_HOME/bin/:$PATH"
```
## Install Scala

This page shows that the current version for Scala 2 is 2.13.10. So I run this command.
```
brew install scala@2.13
export PATH="/opt/homebrew/opt/scala@2.13/bin:$PATH"
```

## Instal Apache Spark
In my case Apache Spark has been installed to
```
export SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.5.0/libexec"
export PATH="$SPARK_HOME/bin/:$PATH"

```
Finally, I think we should run this command: ```source ~/.zshrc```

## Testing Spark
Execute ```spark-shell```
Get the following screen:

 and run the following in scala:
```
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/spark_1.png)

To close Spark shell, you press kbd:```[Ctrl+D]``` or type in ```:quit``` or ```:q```.

## InstallPySpark
[link to installation](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)
To run PySpark, we first need to add it to PYTHONPATH:

```
export PYTHONPATH="/opt/homebrew/Cellar/python@3.12/3.12.1"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```
Make sure that the version under ```${SPARK_HOME}/python/lib/ ```matches the filename of ```py4j``` or you will encounter ```ModuleNotFoundError: No module named 'py4j'``` while executing ```import pyspark```.

Finally, I think we have to run this command: ```source ~/.zshrc```.

## 5.3 Spark SQL and DataFrames
### 5.3.1 First Look at Spark/PySpark

Run Jupyter - in terminal or VSC run command
```Jupyter notebook```

Download a CSV file that we'll use for testing:
```
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```
Now let's run ipython (or jupyter notebook) and execute:
```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()
```
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/spark_2.png)
Test that writing works as well:
```
df.write.parquet('zones')
```
The ```zones``` folder has been created with parquet file inside.



Start Spark on port ```http://localhost:4040/``` or ```http://localhost:4041/jobs/```

All further instuctions in the jupyter file ```4_pyspark.ipynb```
The result of spark job of repartition file can be shown in http://tetianas-air.fritz.box:4041/jobs/job/?id=10

![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/spark_3.png)
![](https://github.com/TOmelchenko/DataEngZoomCamp2024/blob/main/img/spark_4.png)

### 5.3.2 Spark DataFrames
#### Prepare the data
Edit and change the **URL_PREFIX** of the

Modify ```code/download_data.sh``` file like this.
File ```download_data.sh```
```

set -e

TAXI_TYPE="yellow" #"green"
YEAR=2020 #2021

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

done
```

Run bash script with these two commands" 

```
chmod +x download_data.sh # adding permissions to execute

./download_data.sh # run file
```
To load data into spark df in Jupyter, create a new note with Python 3 (ipykernel) and run the code below.

File ```05_taxi_schema.ipynb```

To show mail SQL in Jupyter, create a new note with Python 3 (ipykernel) with this code (or simply open 06_spark.sql.ipynb).

File ```06_spark.sql.ipynb```









