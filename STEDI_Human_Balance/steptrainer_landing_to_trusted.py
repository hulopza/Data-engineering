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

# Script generated for node S3 customer curated
S3customercurated_node1676423822877 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3customercurated_node1676423822877",
)

# Script generated for node S3 - steptrainer
S3steptrainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/stepTrainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3steptrainer_node1",
)

# Script generated for node Join tables
Jointables_node2 = Join.apply(
    frame1=S3steptrainer_node1,
    frame2=S3customercurated_node1676423822877,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Jointables_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://hulopza-lakehouse/stepTrainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(Jointables_node2)
job.commit()
