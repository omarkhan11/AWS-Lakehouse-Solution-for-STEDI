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

# Script generated for node accelerometer trusted
accelerometertrusted_node1741453376302 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1741453376302")

# Script generated for node customer trusted
customertrusted_node1741453366232 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="customer_trusted", transformation_ctx="customertrusted_node1741453366232")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
distinct c.customername as customer_name,
c.email,
c.phone,
c.birthday,
c.serialnumber as serial_number,
c.registrationdate as registration_date, 
c.lastupdatedate as last_update_date,
c.sharewithresearchasofdate as share_with_research_as_of_date,
c.sharewithpublicasofdate as share_with_public_as_of_date,
c.sharewithfriendsasofdate as share_with_friends_as_of_date

FROM customer c inner join acc a on c.email = a.user
'''
SQLQuery_node1741453402288 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":customertrusted_node1741453366232, "acc":accelerometertrusted_node1741453376302}, transformation_ctx = "SQLQuery_node1741453402288")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741453402288, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741453532182", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1741453553043 = glueContext.getSink(path="s3://stedidb/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1741453553043")
customercurated_node1741453553043.setCatalogInfo(catalogDatabase="curated",catalogTableName="customer_curated")
customercurated_node1741453553043.setFormat("json")
customercurated_node1741453553043.writeFrame(SQLQuery_node1741453402288)
job.commit()
