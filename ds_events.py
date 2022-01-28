import sys
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql import *
import requests
from data_checks import *

# from commerce_cube_glue_core import *

from datetime import datetime, timedelta
from commerce_cube_glue_core import mysqlDB, py_to_slack, slack_user_id
from pyspark.sql import functions as sp_functions, types as sp_types

args = getResolvedOptions(sys.argv, ["TempDir", "JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_name = args["JOB_NAME"]


destination_connection_name = "commcube_master_vpc"
#1 Find or create & maintain list of tables
#2 create dictionary/list of tables

dest_table_name = "advertiser_events_latest"
{table_name} + _events
merge_table_name = "advertiser_events"
destination_db_name = "event_bus"

#################################################CONNECTION DETAILS#############################################################################
response = glueContext.extract_jdbc_conf(destination_connection_name)
connection_string = response["url"]
connection_host = connection_string.split("//")[1].split(":")[0]
connection_user = response["user"]
connection_pword = response["password"]
connection_port = connection_string.split("//")[1].split(":")[1]


curr_year = (datetime.now() - timedelta(days=1)).year
curr_month = (datetime.now() - timedelta(days=1)).month
curr_day = (datetime.now() - timedelta(days=1)).day

#make dynamic (loop thrugh?)
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="event_bus",
    table_name="event_bus_advertiser_prod",
    transformation_ctx="datasource0",
    push_down_predicate=f"""(
            CAST(partition_3 AS INT) = {curr_year}
            and CAST(partition_4 AS INT) = {curr_month}
            and CAST(partition_5 AS INT) = {curr_day}
        )""",
)

#create table to store mapping?
#if the source changes, we should track and update our table/s3 doc?
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("event", "string", "event", "string"),
        ("venture_config_id", "string", "venture_config_id", "string"),
        ("venture_reference", "string", "venture_reference", "string"),
        ("created_at", "string", "event_created_at", "string"),
        ("culture", "string", "culture", "string"),
        ("action_type", "string", "action_type", "string"),
        ("action_reference", "string", "action_reference", "string"),
        ("payload", "string", "payload", "string")
    ],
    transformation_ctx="applymapping1",
)


# print( applymapping1.printSchema() )
# FILTER OUT NONE LISTING EVENTS
# listings_events = Filter.apply(frame = applymapping1, f = lambda x: x["event"] not in ["UserLogin"])

# print(listings_events.printSchema() )


#Go through all ds_someting_events jobs and confirm if the below are similar/the same
# If the same (exact), then we can create a reusable method/proc



################################################### START OF PySpark Methods ############################################################
start = datetime.now()

df = applymapping1.toDF()

print("unnesting now")

json_schema = spark.read.json(df.rdd.map(lambda row: row.payload)).schema

# print(df.show(2))
df = (
    df.withColumn("payload", from_json(col("payload"), json_schema))
    .select("*", "payload.Advertiser.*")
    .drop("payload")
)
# print( json_schema )
print("Done unnesting data")

# check if there is no data to process if none exit and send
if df.rdd.isEmpty():
    raise Exception(
        f"Warning! : No data to process for date : {curr_year}-{curr_month}-{curr_day}"
    )


def clean_source_dates(col_name):
    base_time = "2000-01-01 00:00:00"
    try:
        t = str(col_name)
        t = t.replace("T", " ")
        time_regex = re.compile(r"\d{4}-\d{2}-\d{2}\s{1,2}\d{2}:\d{2}:\d{2}")
        res = time_regex.search(t)
        if res:
            extracted_time = res.group().strip()
            if datetime.strptime(extracted_time, "%Y-%m-%d %H:%M:%S") < datetime(
                2000, 1, 1, 0, 0, 0
            ):
                return base_time
            return extracted_time
    except Exception as err:
        return base_time


print("cleaning dates")
clean_source_dates_udf = sp_functions.udf(
    lambda x: clean_source_dates(x), sp_types.StringType()
)

source_date_cols = ["event_created_at", "created_at", "updated_at", "expiry_date"]
dates_clean_df = df

for col_name in df.schema.names:
    col_data_type = df.schema[col_name].dataType
    print(col_data_type)
    if (
        isinstance(col_data_type, sp_types.TimestampType)
        or col_name in source_date_cols
    ):
        dates_clean_df = dates_clean_df.withColumn(
            col_name, clean_source_dates_udf(col_name)
        )

# print(dates_clean_df.show(2))

print("done cleaning dates")

for col_name in dates_clean_df.schema.names:
    col_data_type = dates_clean_df.schema[col_name].dataType
    if isinstance(col_data_type, sp_types.ArrayType) or isinstance(
        col_data_type, sp_types.StructType
    ):
        dates_clean_df = dates_clean_df.withColumn(
            col_name, sp_functions.to_json(col_name)
        )
        # dates_clean_df = dates_clean_df.withColumn(col_name, dates_clean_df[col_name].cast(sp_types.StringType()))

print(dates_clean_df.show(2))

end = datetime.now()

################################################### END OF PySpark Methods ############################################################
print(f"process took {(start - end).seconds} seconds")

# print(dates_clean_df.show(2))

clean_frame_dyf = DynamicFrame.fromDF(dates_clean_df, glueContext, "clean_frame_dyf")
# print( clean_frame_dyf.printSchema() )

resolvechoice2 = ResolveChoice.apply(
    frame=clean_frame_dyf, choice="make_cols", transformation_ctx="resolvechoice2"
)

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, transformation_ctx="dropnullfields3"
)


########################################### FIRST TRUNCATE DATA ##################################################
mysql = mysqlDB({"host": connection_host, "user": connection_user, "db": destination_db_name , "password": connection_pword, "port": connection_port  })
query = f"CALL event_bus.truncate_if_exists( '{dest_table_name}' );"
mysql.query(query)

print("before insert")
######################################### INSERT DATA ############################################################
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dropnullfields3,
    catalog_connection=destination_connection_name,
    connection_options={"dbtable": dest_table_name, "database": destination_db_name},
    transformation_ctx="datasink4",
)

########################################### MERGE DATA ##################################################
mysql = mysqlDB({"host": connection_host, "user": connection_user, "db": destination_db_name , "password": connection_pword, "port": connection_port  })
query = f"CALL event_bus.merge_data( '{dest_table_name}', '{merge_table_name}' );"
mysql.query(query)

print("after insert")

