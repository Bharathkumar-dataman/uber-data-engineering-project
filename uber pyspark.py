# Databricks notebook source
df = spark.read.csv( "/FileStore/tables/uber_data.csv"
, header=True , inferSchema=True)
df.show()
display(df)


# COMMAND ----------

df = df.withColumn("tpep_pickup_datetime", col=("tpep_pickup_datetime").cast("timestamp"))
df = df.withColumn("tpep_dropoff_datetime", col=("tpep_dropoff_datetime").cast("timestamp"))


# COMMAND ----------

df = df.dropDuplicates()
display(df)

# COMMAND ----------

df = df.withColumn("trip-id", col("VendorID"))
df.show()
display(df)


# COMMAND ----------

from pyspark.sql.window import Window
windowSpec = Window.orderBy("VendorID", "tpep_pickup_datetime")  # Adjust as needed for unique ordering
df = df.withColumn("trip_id", row_number().over(windowSpec) - 1)
display(df)


# COMMAND ----------

#Extract datetime components for pickup
from pyspark.sql.functions import col, hour, dayofmonth, month, year, dayofweek, row_number
from pyspark.sql.window import Window
import io

datetime_dim = df.select(
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    hour(col("tpep_pickup_datetime")).alias("pick_hour"),
    dayofmonth(col("tpep_pickup_datetime")).alias("pick_day"),
    month(col("tpep_pickup_datetime")).alias("pick_month"),
    year(col("tpep_pickup_datetime")).alias("pick_year"),
    dayofweek(col("tpep_pickup_datetime")).alias("pick_weekday"),
    hour(col("tpep_dropoff_datetime")).alias("drop_hour"),
    dayofmonth(col("tpep_dropoff_datetime")).alias("drop_day"),
    month(col("tpep_dropoff_datetime")).alias("drop_month"),
    year(col("tpep_dropoff_datetime")).alias("drop_year"),
    dayofweek(col("tpep_dropoff_datetime")).alias("drop_weekday")
)

# Add datetime_id column as index
windowSpec = Window.orderBy("tpep_pickup_datetime")  # Order by pickup datetime or any other unique column
datetime_dim = datetime_dim.withColumn("datetime_id", row_number().over(windowSpec) - 1)

# Select and reorder columns
datetime_dim = datetime_dim.select(
    "datetime_id",
    "tpep_pickup_datetime",
    "pick_hour",
    "pick_day",
    "pick_month",
    "pick_year",
    "pick_weekday",
    "tpep_dropoff_datetime",
    "drop_hour",
    "drop_day",
    "drop_month",
    "drop_year",
    "drop_weekday"
)

# Show the first few rows

display(datetime_dim)


# COMMAND ----------



from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number



# Passenger Count Dimension
passenger_count_dim = df.select("passenger_count").distinct()
passenger_count_dim = passenger_count_dim.withColumn("passenger_count_id", row_number().over(Window.orderBy("passenger_count")) - 1)
passenger_count_dim = passenger_count_dim.select("passenger_count_id", "passenger_count")

# Trip Distance Dimension
trip_distance_dim = df.select("trip_distance").distinct()
trip_distance_dim = trip_distance_dim.withColumn("trip_distance_id", row_number().over(Window.orderBy("trip_distance")) - 1)
trip_distance_dim = trip_distance_dim.select("trip_distance_id", "trip_distance")

# Rate Code Dimension
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}

# Broadcast the dictionary for efficient lookups
rate_code_type_broadcast = spark.sparkContext.broadcast(rate_code_type)

def map_rate_code(rate_code_id):
    return rate_code_type_broadcast.value.get(rate_code_id, "Unknown")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

map_rate_code_udf = udf(map_rate_code, StringType())

# Creating the rate_code_dim DataFrame
rate_code_dim = df.select("RatecodeID").distinct()
rate_code_dim = rate_code_dim.withColumn("rate_code_id", row_number().over(Window.orderBy("RatecodeID")) - 1)
rate_code_dim = rate_code_dim.withColumn("rate_code_name", map_rate_code_udf(col("RatecodeID")))
rate_code_dim = rate_code_dim.select("rate_code_id", "RatecodeID", "rate_code_name")

# Show results
display(passenger_count_dim)
display(trip_distance_dim)
display(rate_code_dim)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Create dimension tables

# Pickup Location Dimension
pickup_location_dim = df.select(col('pickup_longitude'), col('pickup_latitude')).distinct()
pickup_location_dim = pickup_location_dim.withColumn('pickup_location_id', expr('monotonically_increasing_id()'))
pickup_location_dim = pickup_location_dim.select(
    col('pickup_location_id'),
    col('pickup_latitude'),
    col('pickup_longitude')
)

# Dropoff Location Dimension
dropoff_location_dim = df.select(col('dropoff_longitude'), col('dropoff_latitude')).distinct()
dropoff_location_dim = dropoff_location_dim.withColumn('dropoff_location_id', expr('monotonically_increasing_id()'))
dropoff_location_dim = dropoff_location_dim.select(
    col('dropoff_location_id'),
    col('dropoff_latitude'),
    col('dropoff_longitude')
)

# Payment Type Dimension
payment_type_name = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

payment_type_dim = df.select(col('payment_type')).distinct()
payment_type_dim = payment_type_dim.withColumn('payment_type_id', expr('monotonically_increasing_id()'))
payment_type_dim = payment_type_dim.withColumn(
    'payment_type_name',
    when(col('payment_type') == 1, lit('Credit card'))
    .when(col('payment_type') == 2, lit('Cash'))
    .when(col('payment_type') == 3, lit('No charge'))
    .when(col('payment_type') == 4, lit('Dispute'))
    .when(col('payment_type') == 5, lit('Unknown'))
    .when(col('payment_type') == 6, lit('Voided trip'))
    .otherwise(lit('Unknown'))
)
payment_type_dim = payment_type_dim.select(
    col('payment_type_id'),
    col('payment_type'),
    col('payment_type_name')
)

# Fact Table
fact_table = df.join(passenger_count_dim, df['trip_id'] == passenger_count_dim['passenger_count_id'], 'left') \
               .join(trip_distance_dim, df['trip_id'] == trip_distance_dim['trip_distance_id'], 'left') \
               .join(rate_code_dim, df['trip_id'] == rate_code_dim['rate_code_id'], 'left') \
               .join(pickup_location_dim, df['trip_id'] == pickup_location_dim['pickup_location_id'], 'left') \
               .join(dropoff_location_dim, df['trip_id'] == dropoff_location_dim['dropoff_location_id'], 'left') \
               .join(datetime_dim, df['trip_id'] == datetime_dim['datetime_id'], 'left') \
               .join(payment_type_dim, df['trip_id'] == payment_type_dim['payment_type_id'], 'left') \
               .select(
                   df['trip_id'], df['VendorID'], df['datetime_id'], df['passenger_count_id'],
                   df['trip_distance_id'], df['rate_code_id'], df['store_and_fwd_flag'],
                   df['pickup_location_id'], df['dropoff_location_id'], df['payment_type_id'],
                   df['fare_amount'], df['extra'], df['mta_tax'], df['tip_amount'],
                   df['tolls_amount'], df['improvement_surcharge'], df['total_amount']
               )
# Show the fact table
fact_table.show()


# COMMAND ----------

df = spark.read.csv( "/FileStore/tables/uber_data.csv"
, header=True , inferSchema=True)
display(df)
df = df.dropDuplicates()
from pyspark.sql.window import Window
windowSpec = Window.orderBy("VendorID", "tpep_pickup_datetime")  # Adjust as needed for unique ordering
df = df.withColumn("trip_id", row_number().over(windowSpec) - 1)
display(df)
#Extract datetime components for pickup
from pyspark.sql.functions import col, hour, dayofmonth, month, year, dayofweek, row_number
from pyspark.sql.window import Window
import io

datetime_dim = df.select(
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    hour(col("tpep_pickup_datetime")).alias("pick_hour"),
    dayofmonth(col("tpep_pickup_datetime")).alias("pick_day"),
    month(col("tpep_pickup_datetime")).alias("pick_month"),
    year(col("tpep_pickup_datetime")).alias("pick_year"),
    dayofweek(col("tpep_pickup_datetime")).alias("pick_weekday"),
    hour(col("tpep_dropoff_datetime")).alias("drop_hour"),
    dayofmonth(col("tpep_dropoff_datetime")).alias("drop_day"),
    month(col("tpep_dropoff_datetime")).alias("drop_month"),
    year(col("tpep_dropoff_datetime")).alias("drop_year"),
    dayofweek(col("tpep_dropoff_datetime")).alias("drop_weekday")
)

# Add datetime_id column as index
windowSpec = Window.orderBy("tpep_pickup_datetime")  # Order by pickup datetime or any other unique column
datetime_dim = datetime_dim.withColumn("datetime_id", row_number().over(windowSpec) - 1)

# Select and reorder columns
datetime_dim = datetime_dim.select(
    "datetime_id",
    "tpep_pickup_datetime",
    "pick_hour",
    "pick_day",
    "pick_month",
    "pick_year",
    "pick_weekday",
    "tpep_dropoff_datetime",
    "drop_hour",
    "drop_day",
    "drop_month",
    "drop_year",
    "drop_weekday"
)

# Show the first few rows

display(datetime_dim)

from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number



# Passenger Count Dimension
passenger_count_dim = df.select("passenger_count").distinct()
passenger_count_dim = passenger_count_dim.withColumn("passenger_count_id", row_number().over(Window.orderBy("passenger_count")) - 1)
passenger_count_dim = passenger_count_dim.select("passenger_count_id", "passenger_count")

# Trip Distance Dimension
trip_distance_dim = df.select("trip_distance").distinct()
trip_distance_dim = trip_distance_dim.withColumn("trip_distance_id", row_number().over(Window.orderBy("trip_distance")) - 1)
trip_distance_dim = trip_distance_dim.select("trip_distance_id", "trip_distance")

# Rate Code Dimension
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}

# Broadcast the dictionary for efficient lookups
rate_code_type_broadcast = spark.sparkContext.broadcast(rate_code_type)

def map_rate_code(rate_code_id):
    return rate_code_type_broadcast.value.get(rate_code_id, "Unknown")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

map_rate_code_udf = udf(map_rate_code, StringType())

# Creating the rate_code_dim DataFrame
rate_code_dim = df.select("RatecodeID").distinct()
rate_code_dim = rate_code_dim.withColumn("rate_code_id", row_number().over(Window.orderBy("RatecodeID")) - 1)
rate_code_dim = rate_code_dim.withColumn("rate_code_name", map_rate_code_udf(col("RatecodeID")))
rate_code_dim = rate_code_dim.select("rate_code_id", "RatecodeID", "rate_code_name")

# Show results
display(passenger_count_dim)
display(trip_distance_dim)
display(rate_code_dim)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when



# Create dimension tables

# Pickup Location Dimension
pickup_location_dim = df.select(col('pickup_longitude'), col('pickup_latitude')).distinct()
pickup_location_dim = pickup_location_dim.withColumn('pickup_location_id', expr('monotonically_increasing_id()'))
pickup_location_dim = pickup_location_dim.select(
    col('pickup_location_id'),
    col('pickup_latitude'),
    col('pickup_longitude')
)

# Dropoff Location Dimension
dropoff_location_dim = df.select(col('dropoff_longitude'), col('dropoff_latitude')).distinct()
dropoff_location_dim = dropoff_location_dim.withColumn('dropoff_location_id', expr('monotonically_increasing_id()'))
dropoff_location_dim = dropoff_location_dim.select(
    col('dropoff_location_id'),
    col('dropoff_latitude'),
    col('dropoff_longitude')
)

# Payment Type Dimension
payment_type_name = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

payment_type_dim = df.select(col('payment_type')).distinct()
payment_type_dim = payment_type_dim.withColumn('payment_type_id', expr('monotonically_increasing_id()'))
payment_type_dim = payment_type_dim.withColumn(
    'payment_type_name',
    when(col('payment_type') == 1, lit('Credit card'))
    .when(col('payment_type') == 2, lit('Cash'))
    .when(col('payment_type') == 3, lit('No charge'))
    .when(col('payment_type') == 4, lit('Dispute'))
    .when(col('payment_type') == 5, lit('Unknown'))
    .when(col('payment_type') == 6, lit('Voided trip'))
    .otherwise(lit('Unknown'))
)
payment_type_dim = payment_type_dim.select(
    col('payment_type_id'),
    col('payment_type'),
    col('payment_type_name')
)

# Fact Table
fact_table = df.join(passenger_count_dim, df['trip_id'] == passenger_count_dim['passenger_count_id'], 'left') \
               .join(trip_distance_dim, df['trip_id'] == trip_distance_dim['trip_distance_id'], 'left') \
               .join(rate_code_dim, df['trip_id'] == rate_code_dim['rate_code_id'], 'left') \
               .join(pickup_location_dim, df['trip_id'] == pickup_location_dim['pickup_location_id'], 'left') \
               .join(dropoff_location_dim, df['trip_id'] == dropoff_location_dim['dropoff_location_id'], 'left') \
               .join(datetime_dim, df['trip_id'] == datetime_dim['datetime_id'], 'left') \
               .join(payment_type_dim, df['trip_id'] == payment_type_dim['payment_type_id'], 'left') \
               .select(
                   df['trip_id'], df['VendorID'], df['datetime_id'], df['passenger_count_id'],
                   df['trip_distance_id'], df['rate_code_id'], df['store_and_fwd_flag'],
                   df['pickup_location_id'], df['dropoff_location_id'], df['payment_type_id'],
                   df['fare_amount'], df['extra'], df['mta_tax'], df['tip_amount'],
                   df['tolls_amount'], df['improvement_surcharge'], df['total_amount']
               )
# Show the fact table
display(fact_table)



