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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Script generated for node Privacy Join
PrivacyJoin_node3 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node2,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin_node3",
)

# Script generated for node Drop Fields
DropFields_node4 = DropFields.apply(
    frame=PrivacyJoin_node3,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node4",
)

# Script generated for node Customer Curated
CustomerCurated_node5 = glueContext.getSink(
    path="s3://udacity-project-jimmy/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node5",
)
CustomerCurated_node5.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node5.setFormat("json")
CustomerCurated_node5.writeFrame(DropFields_node4)

job.commit()