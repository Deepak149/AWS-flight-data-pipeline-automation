import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

AWSGlueDataCatalog_node1721779081278 = glueContext.create_dynamic_frame.from_catalog(database="airline_datamart", table_name="daily_flights", transformation_ctx="AWSGlueDataCatalog_node1721779081278")

AWSGlueDataCatalog_node1721779984427 = glueContext.create_dynamic_frame.from_catalog(database="airline_datamart", table_name="dev_airlines_airports_dim", transformation_ctx="AWSGlueDataCatalog_node1721779984427", redshift_tmp_dir="s3://airlines-data-processing-temp")

Filter_node1721779116594 = Filter.apply(frame=AWSGlueDataCatalog_node1721779081278, f=lambda row: (row["depdelay"] >= 20), transformation_ctx="Filter_node1721779116594")

Filter_node1721779116594DF = Filter_node1721779116594.toDF()
AWSGlueDataCatalog_node1721779984427DF = AWSGlueDataCatalog_node1721779984427.toDF()
join_dep_airport_node1721780122247 = DynamicFrame.fromDF(Filter_node1721779116594DF.join(AWSGlueDataCatalog_node1721779984427DF, (Filter_node1721779116594DF['destairportid'] == AWSGlueDataCatalog_node1721779984427DF['airport_id']), "left"), glueContext, "join_dep_airport_node1721780122247")

modify_departure_airport_columns_node1721780242489 = ApplyMapping.apply(frame=join_dep_airport_node1721780122247, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modify_departure_airport_columns_node1721780242489")

modify_departure_airport_columns_node1721780242489DF = modify_departure_airport_columns_node1721780242489.toDF()
AWSGlueDataCatalog_node1721779984427DF = AWSGlueDataCatalog_node1721779984427.toDF()
join_arr_airport_node1721780687518 = DynamicFrame.fromDF(modify_departure_airport_columns_node1721780242489DF.join(AWSGlueDataCatalog_node1721779984427DF, (modify_departure_airport_columns_node1721780242489DF['destairportid'] == AWSGlueDataCatalog_node1721779984427DF['airport_id']), "left"), glueContext, "join_arr_airport_node1721780687518")

modify_arrival_airport_columns_node1721780771424 = ApplyMapping.apply(frame=join_arr_airport_node1721780687518, mappings=[("carrier", "string", "carrier", "string"), ("dep_state", "string", "dep_state", "string"), ("state", "string", "arr_state", "string"), ("arr_delay", "bigint", "arr_delay", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_delay", "bigint", "dep_delay", "long"), ("dep_airport", "string", "dep_airport", "string")], transformation_ctx="modify_arrival_airport_columns_node1721780771424")

redshift_target_table_node1721780962091 = glueContext.write_dynamic_frame.from_catalog(frame=modify_arrival_airport_columns_node1721780771424, database="airline_datamart", table_name="dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://airlines-data-processing-temp",additional_options={"aws_iam_role": "arn:aws:iam::381492248270:role/redshift_iam_role"}, transformation_ctx="redshift_target_table_node1721780962091")

job.commit()
