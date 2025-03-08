import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer curated
customercurated_node1741454476389 = glueContext.create_dynamic_frame.from_catalog(database="curated", table_name="customer_curated", transformation_ctx="customercurated_node1741454476389")

# Script generated for node step trainer landing
steptrainerlanding_node1741454495304 = glueContext.create_dynamic_frame.from_catalog(database="landing", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1741454495304")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
s.sensorreadingtime as sensor_reading_time,
s.serialnumber as serial_number, 
s.distancefromobject as distance_from_object

FROM step s inner join cust c on s.serialnumber = c.serial_number
'''
SQLQuery_node1741454510445 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":steptrainerlanding_node1741454495304, "cust":customercurated_node1741454476389}, transformation_ctx = "SQLQuery_node1741454510445")

# Script generated for node Change Schema
ChangeSchema_node1741454538375 = ApplyMapping.apply(frame=SQLQuery_node1741454510445, mappings=[("sensor_reading_time", "long", "sensor_reading_time", "timestamp"), ("serial_number", "string", "serial_number", "string"), ("distance_from_object", "int", "distance_from_object", "int")], transformation_ctx="ChangeSchema_node1741454538375")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1741454538375, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741454467552", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741454579359 = glueContext.getSink(path="s3://stedidb/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1741454579359")
AmazonS3_node1741454579359.setCatalogInfo(catalogDatabase="trusted",catalogTableName="step_trainer_trusted")
AmazonS3_node1741454579359.setFormat("json")
AmazonS3_node1741454579359.writeFrame(ChangeSchema_node1741454538375)
job.commit()
