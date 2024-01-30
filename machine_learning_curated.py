import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import hashlib

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node StepTrainer_Trusted
StepTrainer_Trusted_node1706376677900 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aktestbucket8964/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_Trusted_node1706376677900",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1706376830069 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometer_Trusted_node1706376830069",
)

# Script generated for node Join
Join_node1706376877774 = Join.apply(
    frame1=Accelerometer_Trusted_node1706376830069,
    frame2=StepTrainer_Trusted_node1706376677900,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1706376877774",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    Join_node1706376877774, ["EMAIL"], 1.0, 0.1, "HIGH"
)


def pii_column_hash(original_cell_value):
    return hashlib.sha256(str(original_cell_value).encode()).hexdigest()


pii_column_hash_udf = udf(pii_column_hash, StringType())


def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")


DetectSensitiveData_node1706384233248 = hashDf(
    Join_node1706376877774, list(classified_map.keys())
)

# Script generated for node Machine_Learning_Curated
Machine_Learning_Curated_node1706376934111 = glueContext.getSink(
    path="s3://aktestbucket8964/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Machine_Learning_Curated_node1706376934111",
)
Machine_Learning_Curated_node1706376934111.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
Machine_Learning_Curated_node1706376934111.setFormat("json")
Machine_Learning_Curated_node1706376934111.writeFrame(
    DetectSensitiveData_node1706384233248
)
job.commit()
