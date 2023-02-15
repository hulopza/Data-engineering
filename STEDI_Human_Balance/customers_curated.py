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

# Script generated for node S3 bucket - customer trusted
S3bucketcustomertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketcustomertrusted_node1",
)

# Script generated for node Has data filter
Hasdatafilter_node2 = Filter.apply(
    frame=S3bucketcustomertrusted_node1,
    f=lambda row: (not (row["z"] == 0) and not (row["y"] == 0) and not (row["x"] == 0)),
    transformation_ctx="Hasdatafilter_node2",
)

# Script generated for node S3 bucket - curated data
S3bucketcurateddata_node3 = glueContext.getSink(
    path="s3://hulopza-lakehosue/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucketcurateddata_node3",
)
S3bucketcurateddata_node3.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="customers_curated"
)
S3bucketcurateddata_node3.setFormat("json")
S3bucketcurateddata_node3.writeFrame(Hasdatafilter_node2)
job.commit()
