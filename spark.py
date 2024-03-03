import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [2]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Get Spark context
sc = SparkContext()
# From spark context get glue context and spark session
glueContext = GlueContext(sc)
# Create and init job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Begin TODOs - add your code starting from here. Comments
# are provided for each statement that you may need to add.

# 1. Create a Glue client to access the Data Catalog API
client = boto3.client('glue')

# 2. Create a dynamic frame from AWS Glue catalog table. In the following lines
# use the create_dynamic_frame.from_catalog() API of the GlueContext class. Use
# the Glue catalog database and table name (output of job 1) as arguments.
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="test-flights-db", table_name="filtered", transformation_ctx="datasource0")
# datasource0.show(10)
# datasource0.printSchema()
# datasource0.count()

# 3. Get Spark dataframe from the Glue dynamic frame created above
datasource1=datasource0.toDF()

# 4. Create a new time_zone_difference column and add it to the Spark data frame.
# See the MP description on how to calculate the value of the time zone
# difference between the arrival and departure airports. You may need to check the
# data type when doing the time zone difference calculations to get the correct values.

# data = data.withColumn("scheduled_arrival", col("scheduled_arrival").cast("bigint"))
# data = data.withColumn("scheduled_departure", col("scheduled_departure").cast("bigint"))
# data = data.withColumn("scheduled_time", col("scheduled_time").cast("bigint"))

new_time_zone_diff = (
    (datasource1.scheduled_arrival / 100) * 60 +
    (datasource1.scheduled_arrival % 100) -
    ((datasource1.scheduled_departure / 100) * 60 +
     (datasource1.scheduled_departure % 100) +
     datasource1.scheduled_time) % (24 * 60)
)

#datasource1 = datasource1.withColumn('time_zone_difference', lit(new_time_zone_diff))
datasource1 = datasource1.withColumn('time_zone_difference', new_time_zone_diff.cast(IntegerType()))  

# print(datasource1.collect())
# print(datasource1.show())
# # datasource1.select_fields(['time_zone_difference', 'year'])
# datasource1.show(3)

# 5. Convert Spark data frame back to Glue dynamic frame
# Note - you can do step 4 using AWS Glue dynamic frame APIs also if you want
# to avoid steps 3 and 5. However, it maybe easier to do the transformations
# in step 4 using Spark data frame.
datasource2 = DynamicFrame.fromDF(datasource1, glueContext, "datasource2") 

# 6. Get the existing Glue catalog table schema. You can use the glue client
# created in step 1 and use its get_table() API to get the table schema which
# will be a python dictionary. You can see the response of get_table() here:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_table.html
table_schema_dict = client.get_table(
    DatabaseName='test-flights-db',
    Name='filtered')
print('table_schema_dict', table_schema_dict)
# 7. Delete the following fields in the table schema dictionary as
# the update_table API gives ParamValidationError when these fields are present:
# 'UpdateTime', 'IsRegisteredWithLakeFormation', 'CreatedBy', 'DatabaseName',
# 'CreateTime', 'CatalogId'. If there is an error related to 'VersionId', that
# field also needs to be deleted.

keys_to_remove = ['UpdateTime', 'IsRegisteredWithLakeFormation', 'CreatedBy', 'DatabaseName', 'CreateTime', 'CatalogId', 'VersionId']

for key in keys_to_remove:
    table_schema_dict.pop(key, None)

# 8. Define the new column 'time_zone_difference' to be added to the table schema
col_time_zone_diff = {'Name': 'time_zone_difference', 'Type': 'bigint'}
print('col_time_zone_diff', col_time_zone_diff)

# 9. Append the new column info to the table dictionary (obtained in step 6) columns list
if table_schema_dict['Table'] is not None and table_schema_dict['Table']['StorageDescriptor'] is not None:
    table_schema_dict['Table']['StorageDescriptor']['Columns'].append(col_time_zone_diff)
print('table_schema_dict', table_schema_dict)

# 10. Update the table with the new schema. Use the update_table() API of glue client:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/update_table.html
table = table_schema_dict['Table']
print('table', table)
response = client.update_table(
    DatabaseName=table['DatabaseName'],
    TableInput= {
        'Name': 'time_diff',
        'StorageDescriptor': {
            'Columns': [
                  {
                    "Name": "year",
                    "Type": "bigint"
                  },
                  {
                    "Name": "day",
                    "Type": "bigint"
                  },
                  {
                    "Name": "origin_airport",
                    "Type": "string"
                  },
                  {
                    "Name": "departure_delay",
                    "Type": "bigint"
                  },
                  {
                    "Name": "scheduled_time",
                    "Type": "bigint"
                  },
                  {
                    "Name": "cancelled",
                    "Type": "bigint"
                  },
                  {
                    "Name": "elapsed_time",
                    "Type": "bigint"
                  },
                  {
                    "Name": "diverted",
                    "Type": "bigint"
                  },
                  {
                    "Name": "destination_airport",
                    "Type": "string"
                  },
                  {
                    "Name": "departure_time",
                    "Type": "bigint"
                  },
                  {
                    "Name": "arrival_delay",
                    "Type": "bigint",
                    "Comment": ""
                  },
                  {
                    "Name": "scheduled_departure",
                    "Type": "bigint"
                  },
                  {
                    "Name": "arrival_time",
                    "Type": "bigint"
                  },
                  {
                    "Name": "month",
                    "Type": "bigint"
                  },
                  {
                    "Name": "airline",
                    "Type": "string"
                  },
                  {
                    "Name": "scheduled_arrival",
                    "Type": "bigint"
                  },
                {
                    "Name": "time_zone_difference",
                    "Type": "bigint"
                  }
            ],
        }
    }
    )
    
# 11. Get the output S3 bucket in which the transformed table data will be
# stored. Use the getSink() API of the GlueContext class.

sink = glueContext.getSink(
    connection_type="s3", 
    path="s3://mp10-bucket/time_diff_column/",
    enableUpdateCatalog=True, 
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[])

# 12. Set the catalog database and table using the setCatalogInfo() API on
# the object obtained in step 11.
# 13. Set the format to 'json' using setFormat() API
# 14. Write data into S3 bucket using writeFrame()
# s3_data_sink.writeFrame(datasource2)

#doesn't work
sink.setCatalogInfo(catalogDatabase="test-flights-db", catalogTableName="time_diff")
sink.setFormat("json")
sink.writeFrame(datasource2)



# End TODOs

# Commit job
job.commit()