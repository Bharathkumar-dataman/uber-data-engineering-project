# uber-data-engineering-project
 ## Azure End- To-End Data Engineering Project: Azure Data Factory, Azure Databricks, Azure Data Lake Gen2, Azure Synapse Analytics, Power BI.


#   Introduction.

 ## In this project, we will demonstrate an  end-to-end data engineering project based on data like uber dataset. We will use Azure Data Factory for data ingestion, Azure Data Lake Gen2 for storage, Azure Databricks for data transformation, Azure Synapse for modeling, and Power BI for visualization.

# Architecture
   ![uber architecture](https://github.com/user-attachments/assets/5b3e39ce-fd87-4631-bc0f-5c982a535f76)

# Technology Used
 ## Azure Cloud Platform
   * Azure Data Factory
   * Azure Data Lake Gen2
   * Azure Databricks(pyspark)
   * Azure Synapse
   * Power BI
   * Azure keyvault
   * Azure Entraid(azure active directory)

# Dataset Used
 ## TLC Trip Record Data Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.  

##  Here is the dataset used in this project https://github.com/Bharathkumar-dataman/uber-data-engineering-project/blob/main/uber_data.csv
   
    ## More info about dataset can be found here:

        ##  Website - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
        ##  Data Dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# Data Model
   ![data_model](https://github.com/user-attachments/assets/6494aa4a-4044-4f71-8918-9ac2feb2bc11)






   

# Project Planning and Definition.

 ## In the following project we will use the following schema with the following order :

# *Data Ingestion (Azure Data Factory):

 ## Azure Data Factory serves as our data ingestion platform. It enables us to collect uber  data , APIs, and web scraping. Data Factory’s data connectors and scheduling capabilities are invaluable for automated ingestion.

# *Data Storage (Azure Data Lake Gen2):

 ## Processed uber data finds its home in Azure Data Lake Gen2. This storage solution provides scalable, secure, and cost-effective storage, which is critical for accommodating the increasing volume of uber  data.

# *Data Transformation (Azure Databricks):

 ## We utilize Azure Databricks for data transformation and processing tasks. Databricks clusters allow us to perform data cleansing, normalization, and feature engineering, preparing the uber  data for analysis.

# *Data Modeling (Azure Synapse):

 ## Azure Synapse serves as our data modeling and analytics platform. We employ it to build data models, perform SQL-based queries, and conduct complex analytical tasks on the uber dataset.

# *Data Visualization (Power BI):

 ## Power BI is the tool of choice for data visualization. We create interactive dashboards and reports to present the uber data insights, enabling stakeholders to make informed decisions.

 ## By orchestrating data flow, transformation, storage, modeling, and visualization using Azure services, we aim to provide actionable insights from this critical dataset.


# Architecture Overview
 ## This architectural overview encapsulates our approach in this data engineering project, emphasizing the role of Azure services in processing and analyzing uber data. The architecture ensures that the data pipeline is efficient, secure, and capable of handling the evolving requirements of uber  data analysis.

# For this project, we initiated by establishing our resource group, within which we proceeded to create the essential resources.


# Resources
 ## Data Collection and Ingestion.

   ## We will extract the data from GitHub, which contains information about TLC Trip Record Data Yellow and green taxi trip records 


#  Raw data
  ## In this phase, we harnessed the power of Azure Data Factory to seamlessly ingest data from the “GitHub” source into our Azure environment.


# Pipeline of copy all data
 ## We commence by establishing our source dataset, configured as an HTTP source. This step encompasses the setup of the Linked service and the definition of the BASE URL for all our files. In the case of the sink datasets, we select Delimited Text (CSV) format, and for the Linked service, we designate the destination container as “raw-data” within our Storage account. Since the data remains unaltered and requires no transformations, the primary task revolves around copying the files. We apply minor modifications to the file names to improve their clarity and comprehensibility.

# Data Storage.

 ## In this phase, we ensure that our data is meticulously organized within our Azure Data Lake Storage. We have designated a specific container, which we refer to as “raw-data,” 
 ## for this purpose. This staging area acts as the initial repository for our raw data files, providing a secure and organized location for the data to reside Raw data in ADLS Gen2


# Data Transformation.

  ## In the data transformation phase, we leverage Azure Databricks with a notebook environment to write and execute our Spark code. To initiate this process, we’ve provisioned Azure Databricks by creating the required resources and compute instances. However, before delving into coding, it is crucial to establish a secure connection between Azure Databricks and the Azure Storage accounts that house our raw data.

 ## To achieve this, we have developed an application using the ‘App registrations’ resource provided by Azure. Within this application, we’ve generated an ‘Application (client) ID’ and a ‘Directory (tenant) ID.’ For clarity, this application is named ‘uberapp.’

 ## Subsequently, within the ‘Certificates & secrets’ section of application management, we have generated a client secret key (as illustrated in the figure below). This secret key plays a pivotal role in maintaining a secure and robust connection between Azure Databricks and the Azure Storage accounts, enabling seamless data transformation and processing.

## Finally, we must configure role assignments for ‘Storage Blob Data Contributor.’ This assignment allows Azure Databricks to read, write, and delete access to Azure Storage blob containers and data, facilitating efficient data management and processing.”


# The Secret Key
 ## So now, we have commenced writing our Spark code in the notebook, beginning with the establishment of the connection between the container in the storage accounts and our notebook.

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "client ID",
"fs.azure.account.oauth2.client.secret": 'Secret Key',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/Directory (tenant) ID/oauth2/token"}



dbutils.fs.mount(
source = "abfss://raw-data@<Storage Account name>.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/raw-data",
extra_configs = configs)


# transformed data
dbutils.fs.mount(
source = "abfss://transformed-data@<Storage Account name>.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/transformed-data",
extra_configs = configs)
After this, we can list the files and directories in the ‘raw data’ container by using the following command:

%fs
ls "/mnt/raw-data"
# After that, we should configure our Spark session using the command: Spark. Now, we can begin our actual data work by first reading the data:

# read the data and create a dataframe 
  ## Databricks notebook source
df = spark.read.csv( "/FileStore/tables/uber_data.csv"
, header=True , inferSchema=True)
df.show()
display(df

# format pickup datetime to timestamp
  # COMMAND ----------

df = df.withColumn("tpep_pickup_datetime", col=("tpep_pickup_datetime").cast("timestamp"))
df = df.withColumn("tpep_dropoff_datetime", col=("tpep_dropoff_datetime").cast("timestamp"))

# check for any duplicates and remove 
  COMMAND ----------

df = df.dropDuplicates()
display(df)

# add new column 
# COMMAND ----------

df = df.withColumn("trip-id", col("VendorID"))
df.show()
display(df)


# add new column using window function 
  ## COMMAND ----------

from pyspark.sql.window import Window
windowSpec = Window.orderBy("VendorID", "tpep_pickup_datetime")  # Adjust as needed for unique ordering
df = df.withColumn("trip_id", row_number().over(windowSpec) - 1)
display(df)
![Screenshot (175)](https://github.com/user-attachments/assets/f710f7a9-6ac4-43c1-a434-1c22443c7151)

# Extract datetime components for pickup
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
display(datetime_dim)
![Screenshot (178)](https://github.com/user-attachments/assets/a433020a-298c-429f-88a9-9f768ecfa1dd)


![Screenshot (179)](https://github.com/user-attachments/assets/7f58c32a-d494-4acc-9ba3-18d3aa76941d)





 ## We can utilize various PySpark SQL DataFrame methods, such as .show() and .printSchema(), to view and gain a better understanding of the data.

  ## After reviewing the data and considering our requirements, we have decided to extract  dimension tables from our dataset. 

# let's process and transform the uber data

COMMAND ----------



from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number



# Passenger Count Dimension
passenger_count_dim = df.select("passenger_count").distinct()
passenger_count_dim = passenger_count_dim.withColumn("passenger_count_id", row_number().over(Window.orderBy("passenger_count")) - 1)
passenger_count_dim = passenger_count_dim.select("passenger_count_id", "passenger_count")

## The data in the ‘passenger_count_dim’ table will be structured as follows:



![Screenshot (183)](https://github.com/user-attachments/assets/cc6d6b28-77dc-4830-8ad5-da131878f1ca)


# Trip Distance Dimension
trip_distance_dim = df.select("trip_distance").distinct()
trip_distance_dim = trip_distance_dim.withColumn("trip_distance_id", row_number().over(Window.orderBy("trip_distance")) - 1)
trip_distance_dim = trip_distance_dim.select("trip_distance_id", "trip_distance")

 ## The data in the ‘trip_distance_dim’ table will be structured as follows:

![Screenshot (184)](https://github.com/user-attachments/assets/98886df9-e037-44b1-975a-acd8dd2ddc3f)


# Rate Code Dimension
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}

## The data in the ‘Rate code dim’ table will be structured as follows:
![Screenshot (185)](https://github.com/user-attachments/assets/87a97bd8-334b-4d0d-b6ec-f8a1ead60ca6)




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



# Create dimension tables
# pyspark code used for transformation you can check: https://github.com/Bharathkumar-dataman/uber-data-engineering-project/blob/main/uber%20pyspark.py

# Pickup Location Dimension
# Dropoff Location Dimension
# Payment Type Dimension

# Fact Table(after creating dimension tables as above join with fact table , u can also use data modeling for proper understanding 
)

![Screenshot (187)](https://github.com/user-attachments/assets/6b10d5b7-f17c-4f45-8724-1c507e76657b)

![Screenshot (188)](https://github.com/user-attachments/assets/7b670a63-08ec-44e5-af57-d0d1b1da06c6)



Now that we know the dimensions table and the fact table, we should write these tables into our container named ‘transformed data’ :

# Write our files
passenger_count_dim.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/passenger_count_dim")
trip_distance_dim.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/trip_distance_dim")
Pickup _Location_dim.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/Picku _Location_dim")
Dropoff_Location_dim.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/Dropoff_ Location_dim")
payment_type_dim.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/payment_type_dim")
fact_table.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/transformed-data/fact_table")

## So far, so good. Now we can locate our files in our sink container named ‘transformed-data’:


# Transformed data container
# Data Modeling.

 ## Now that we have our data in the tables, we will proceed to load it into the Lake Database in Azure Synapse Analytics, enabling us to create our models.

 ## First, we need to set up our Azure Synapse workspace. By creating our Synapse Studio, we also create another Storage Account: Azure Data Lake Storage Gen2 (ADLS Gen2).

 ## To use Azure Synapse for working with this data, we should copy the files from the ‘Transformed-data’ container into our ADLS Gen2. For this purpose, we will utilize a pipeline containing a copy activity from our source with the linked service: AzureBlobStorage, to our destination with the linked service: Default Storage account for our Synapse workspace (ADLS Gen2).

 ## Another tip: to copy all the files in the ‘transformed-data’ container, rather than one file at a time, we can utilize the ‘Wildcard File Path’ option with the input as ‘transformed-data/*’.

## Now, in the data part of the Synapse workspace, we add a Lake Database named ‘uberdb’ Following this, we create external tables from the data lake. To do this, we specify the External table name (which will be the same as ‘passenger_dim,’ ‘trip_distance_date,’ etc.), the Linked service (which will be ‘ADLS Gen2’), and the Input file or folder. This input will specify the path to the files.

## In the phase of creating our tables in the Lake database, we have recently discovered that the files in the “fact_table” folder have been duplicated four times due to their size.


# Folder Fact table
  
  ## We will now implement a different pipeline to consolidate the files within the “fact_table” folder. This new pipeline will consist of a single activity: data copying. In this pipeline, we will use the wildcard path option directly targeting our “fact_table” folder. Additionally, we will modify the sink settings by choosing the “merge files” copy behavior.




# The database uberdb

  ## We will now establish relationships between the tables. A relationship is a connection that defines how the data in these tables should be associated or correlated.

  ## We chose the “To table” option for the fact tables, as these table serve as the parent tables for the dimension tables.

# create table in synaspe analytics  
   ## i have provided seperate file for synaspe sql query for understandle approch  here is link -- https://github.com/Bharathkumar-dataman/uber-data-engineering-project/blob/main/synaspe.analytics.sql.query.txt



# Data Visualization 

## After applying different complex queries uber data ready for visualization using power BI
## anyone can contribute to this  project for interactive data visulizations using Power BI  here is my uber data engineering project link : https://github.com/Bharathkumar-dataman/uber-data-engineering-project
## let's connect!


