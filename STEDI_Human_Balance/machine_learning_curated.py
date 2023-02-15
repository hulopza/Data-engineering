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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/stepTrainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1676501151600 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1676501151600",
)

# Script generated for node Join
Join_node1676501208603 = Join.apply(
    frame1=accelerometer_trusted_node1676501151600,
    frame2=step_trainer_trusted_node1,
    keys1=["timeStamp"],
    keys2=["`(right) sensorReadingTime`"],
    transformation_ctx="Join_node1676501208603",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1676501377310 = glueContext.getSink(
    path="s3://hulopza-lakehouse/stepTrainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1676501377310",
)
machine_learning_curated_node1676501377310.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1676501377310.setFormat("json")
machine_learning_curated_node1676501377310.writeFrame(Join_node1676501208603)
job.commit()
