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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1741403189714 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedidb/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1741403189714")

# Script generated for node Customer Trusted
CustomerTrusted_node1741403178594 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1741403178594")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
distinct a.user,
a.timestamp, 
a.x,
a.y,
a.z
from acc a
inner join cus c on a.user = c.email
where c.sharewithresearchasofdate is not null
'''
SQLQuery_node1741404728101 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"acc":AccelerometerLanding_node1741403189714, "cus":CustomerTrusted_node1741403178594}, transformation_ctx = "SQLQuery_node1741404728101")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741404728101, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741404658335", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1741404899094 = glueContext.getSink(path="s3://stedidb/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1741404899094")
AccelerometerTrusted_node1741404899094.setCatalogInfo(catalogDatabase="trusted",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1741404899094.setFormat("json")
AccelerometerTrusted_node1741404899094.writeFrame(SQLQuery_node1741404728101)
job.commit()
