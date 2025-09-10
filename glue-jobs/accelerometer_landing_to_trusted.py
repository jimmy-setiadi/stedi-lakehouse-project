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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-jimmy/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node2",
)

# Script generated for node Privacy Join
PrivacyJoin_node3 = Join.apply(
    frame1=AccelerometerLanding_node2,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyJoin_node3",
)

# Script generated for node Drop Fields
DropFields_node4 = DropFields.apply(
    frame=PrivacyJoin_node3,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node4",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node5 = glueContext.getSink(
    path="s3://udacity-project-jimmy/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node5",
)
AccelerometerTrusted_node5.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node5.setFormat("json")
AccelerometerTrusted_node5.writeFrame(DropFields_node4)

job.commit()