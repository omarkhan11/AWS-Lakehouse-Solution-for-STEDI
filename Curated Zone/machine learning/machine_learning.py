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

# Script generated for node acc trusted
acctrusted_node1741455171740 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="accelerometer_trusted", transformation_ctx="acctrusted_node1741455171740")

# Script generated for node step trainer trusted
steptrainertrusted_node1741455757166 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1741455757166")

# Script generated for node acc 
acc_node1741456390501 = ApplyMapping.apply(frame=acctrusted_node1741455171740, mappings=[("user", "string", "user", "string"), ("timestamp", "long", "timestamp", "timestamp"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="acc_node1741456390501")

# Script generated for node step
step_node1741456403039 = ApplyMapping.apply(frame=steptrainertrusted_node1741455757166, mappings=[("sensor_reading_time", "string", "sensor_reading_time", "timestamp"), ("serial_number", "string", "serial_number", "string"), ("distance_from_object", "int", "distance_from_object", "int")], transformation_ctx="step_node1741456403039")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
s.sensor_reading_time,
s.serial_number,
s.distance_from_object

FROM step s join acc a on s.sensor_reading_time = a.timestamp
'''
SQLQuery_node1741455189199 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"acc":acc_node1741456390501, "step":step_node1741456403039}, transformation_ctx = "SQLQuery_node1741455189199")

# Script generated for node maching learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741455189199, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741455159613", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinglearningcurated_node1741455251269 = glueContext.getSink(path="s3://stedidb/curated/machine learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinglearningcurated_node1741455251269")
machinglearningcurated_node1741455251269.setCatalogInfo(catalogDatabase="curated",catalogTableName="machine_learning_curated")
machinglearningcurated_node1741455251269.setFormat("json")
machinglearningcurated_node1741455251269.writeFrame(SQLQuery_node1741455189199)
job.commit()
