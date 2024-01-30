import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer Curated
CustomerCurated_node1706270073964 = glueContext.getSink(
    path="s3://aktestbucket8964/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1706270073964",
)
CustomerCurated_node1706270073964.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1706270073964.setFormat("json")
CustomerCurated_node1706270073964.writeFrame(DropDuplicates_node1706287388773)
job.commit()
