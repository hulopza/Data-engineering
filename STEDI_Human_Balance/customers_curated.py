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

# Script generated for node Customer
Customer_node1676483040049 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_node1676483040049",
)

# Script generated for node  Accelerometer
Accelerometer_node1676483180214 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_node1676483180214",
)

# Script generated for node Has data
Hasdata_node1676483248307 = Filter.apply(
    frame=Accelerometer_node1676483180214,
    f=lambda row: (not (row["x"] == 0) and not (row["y"] == 0) and not (row["z"] == 0)),
    transformation_ctx="Hasdata_node1676483248307",
)

# Script generated for node Amazon S3
AmazonS3_node1676483349131 = Join.apply(
    frame1=Hasdata_node1676483248307,
    frame2=Customer_node1676483040049,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="AmazonS3_node1676483349131",
)

# Script generated for node Drop accelerometer columns
Dropaccelerometercolumns_node1676483486857 = ApplyMapping.apply(
    frame=AmazonS3_node1676483349131,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="Dropaccelerometercolumns_node1676483486857",
)

# Script generated for node customer curated
customercurated_node1676483594785 = glueContext.getSink(
    path="s3://hulopza-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node1676483594785",
)
customercurated_node1676483594785.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="customers_curated"
)
customercurated_node1676483594785.setFormat("json")
customercurated_node1676483594785.writeFrame(Dropaccelerometercolumns_node1676483486857)
job.commit()
