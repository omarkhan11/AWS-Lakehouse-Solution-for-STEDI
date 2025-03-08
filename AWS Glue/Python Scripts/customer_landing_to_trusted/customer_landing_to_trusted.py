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

# Script generated for node Amazon S3
AmazonS3_node1741401955477 = glueContext.create_dynamic_frame.from_catalog(database="landing", table_name="customer_landing", transformation_ctx="AmazonS3_node1741401955477")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
*
FROM customer_landing
where sharewithresearchasofdate is not null
'''
SQLQuery_node1741402001184 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":AmazonS3_node1741401955477}, transformation_ctx = "SQLQuery_node1741402001184")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741402001184, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741401951669", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1741402205576 = glueContext.getSink(path="s3://stedidb/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1741402205576")
CustomerTrusted_node1741402205576.setCatalogInfo(catalogDatabase="trusted",catalogTableName="customer_trusted")
CustomerTrusted_node1741402205576.setFormat("json")
CustomerTrusted_node1741402205576.writeFrame(SQLQuery_node1741402001184)
job.commit()
