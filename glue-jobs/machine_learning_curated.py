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

# Script generated for node SQL Join
SqlJoin_node3 = sparkSqlQuery(
    glueContext,
    query="SELECT st.sensorreadingtime, st.serialnumber, st.distancefromobject, a.user, a.x, a.y, a.z FROM myDataSource1 st JOIN myDataSource2 a ON st.sensorreadingtime = a.timestamp",
    mapping={
        "myDataSource1": StepTrainerTrusted_node1,
        "myDataSource2": AccelerometerTrusted_node2,
    },
    transformation_ctx="SqlJoin_node3",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node4 = glueContext.getSink(
    path="s3://udacity-project-jimmy/machine-learning/",
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