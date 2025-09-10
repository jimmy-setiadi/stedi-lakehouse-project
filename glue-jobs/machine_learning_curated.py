import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Convert DynamicFrames to DataFrames for SQL operations
step_trainer_df = StepTrainerTrusted_node1.toDF()
accelerometer_df = AccelerometerTrusted_node2.toDF()

# Create temporary views for SQL query
step_trainer_df.createOrReplaceTempView("step_trainer_trusted")
accelerometer_df.createOrReplaceTempView("accelerometer_trusted")

# SQL query with correct lowercase column names
joined_df = spark.sql("""
    SELECT 
        st.sensorreadingtime,
        st.serialnumber,
        st.distancefromobject,
        a.user,
        a.x,
        a.y,
        a.z
    FROM step_trainer_trusted st
    INNER JOIN accelerometer_trusted a 
    ON st.sensorreadingtime = a.timestamp
""")

# Convert DataFrame back to DynamicFrame
SqlJoin_node3 = glueContext.create_dynamic_frame.from_rdd(
    joined_df.rdd,
    name="SqlJoin_node3",
    transformation_ctx="SqlJoin_node3"
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node4 = glueContext.getSink(
    path="s3://udacity-project-jimmy/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node4",
)
MachineLearningCurated_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node4.setFormat("json")
MachineLearningCurated_node4.writeFrame(SqlJoin_node3)

job.commit()
