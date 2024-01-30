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

# Script generated for node Customer_Curated2
Customer_Curated2_node1706374654020 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="Customer_Curated2_node1706374654020",
)

# Script generated for node StepTrainer_Landing
StepTrainer_Landing_node1706363394412 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aktestbucket8964/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_Landing_node1706363394412",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1706375958359 = ApplyMapping.apply(
    frame=Customer_Curated2_node1706374654020,
    mappings=[
        ("customername", "string", "right_customername", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1706375958359",
)

# Script generated for node Join
Join_node1706364820857 = Join.apply(
    frame1=StepTrainer_Landing_node1706363394412,
    frame2=RenamedkeysforJoin_node1706375958359,
    keys1=["serialnumber"],
    keys2=["right_serialnumber"],
    transformation_ctx="Join_node1706364820857",
)

# Script generated for node Drop Fields
DropFields_node1706376001394 = DropFields.apply(
    frame=Join_node1706364820857,
    paths=[
        "right_birthday",
        "right_email",
        "right_sharewithpublicasofdate",
        "right_sharewithresearchasofdate",
        "right_sharewithfriendsasofdate",
        "right_phone",
        "right_serialnumber",
        "right_registrationdate",
        "right_customername",
        "right_lastupdatedate",
    ],
    transformation_ctx="DropFields_node1706376001394",
)

# Script generated for node StepTrainer_Trusted
StepTrainer_Trusted_node1706363946646 = glueContext.getSink(
    path="s3://aktestbucket8964/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainer_Trusted_node1706363946646",
)
StepTrainer_Trusted_node1706363946646.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainer_Trusted_node1706363946646.setFormat("json")
StepTrainer_Trusted_node1706363946646.writeFrame(DropFields_node1706376001394)
job.commit()
