import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1706180154155 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aktestbucket8964/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1706180154155",
)

# Script generated for node ShareWithResearch
ShareWithResearch_node1706180425616 = Filter.apply(
    frame=CustomerLanding_node1706180154155,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="ShareWithResearch_node1706180425616",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1706181402345 = glueContext.getSink(
    path="s3://aktestbucket8964/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1706181402345",
)
CustomerTrusted_node1706181402345.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1706181402345.setFormat("json")
CustomerTrusted_node1706181402345.writeFrame(ShareWithResearch_node1706180425616)
job.commit()


