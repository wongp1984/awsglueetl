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
from pyspark.sql import functions as SqlFuncs
import hashlib

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1706269838561 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingNode_node1706269838561",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1706269660127 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aktestbucket8964/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1706269660127",
)

# Script generated for node Join
Join_node1706269947262 = Join.apply(
    frame1=AccelerometerLandingNode_node1706269838561,
    frame2=CustomerTrustedZone_node1706269660127,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706269947262",
)

# Script generated for node Drop Fields
DropFields_node1706271251596 = DropFields.apply(
    frame=Join_node1706269947262,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1706271251596",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706287388773 = DynamicFrame.fromDF(
    DropFields_node1706271251596.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706287388773",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    DropDuplicates_node1706287388773, ["EMAIL", "PERSON_NAME"], 1.0, 0.1, "HIGH"
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


DetectSensitiveData_node1706383432416 = hashDf(
    DropDuplicates_node1706287388773, list(classified_map.keys())
)

# Script generated for node Customer Curated
CustomerCurated_node1706270073964 = glueContext.getSink(
    path="s3://aktestbucket8964/customer/curated_pii_redacted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1706270073964",
)
CustomerCurated_node1706270073964.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated_pii_redacted"
)
CustomerCurated_node1706270073964.setFormat("json")
CustomerCurated_node1706270073964.writeFrame(DetectSensitiveData_node1706383432416)
job.commit()
