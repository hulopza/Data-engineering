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

# Script generated for node S3 bucket - landing
S3bucketlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketlanding_node1",
)

# Script generated for node Filter research users
Filterresearchusers_node2 = Filter.apply(
    frame=S3bucketlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filterresearchusers_node2",
)

# Script generated for node S3 bucket trusted
S3buckettrusted_node3 = glueContext.getSink(
    path="s3://hulopza-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3buckettrusted_node3",
)
S3buckettrusted_node3.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="customer_trusted"
)
S3buckettrusted_node3.setFormat("json")
S3buckettrusted_node3.writeFrame(Filterresearchusers_node2)
job.commit()
