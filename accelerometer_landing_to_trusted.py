import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
    paths=[
        "serialnumber",
        "birthday",
        "registrationdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1706271251596",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706270073964 = glueContext.getSink(
    path="s3://aktestbucket8964/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1706270073964",
)
AccelerometerTrusted_node1706270073964.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1706270073964.setFormat("json")
AccelerometerTrusted_node1706270073964.writeFrame(DropFields_node1706271251596)
job.commit()
