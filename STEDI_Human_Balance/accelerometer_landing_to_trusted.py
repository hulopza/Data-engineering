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

# Script generated for node Customer S3
CustomerS3_node1676419131677 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerS3_node1676419131677",
)

# Script generated for node Accelerometer S3
AccelerometerS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerS3_node1",
)

# Script generated for node Join approved research
Joinapprovedresearch_node2 = Join.apply(
    frame1=AccelerometerS3_node1,
    frame2=CustomerS3_node1676419131677,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Joinapprovedresearch_node2",
)

# Script generated for node Drop columns
Dropcolumns_node3 = ApplyMapping.apply(
    frame=Joinapprovedresearch_node2,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="Dropcolumns_node3",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1676481954785 = glueContext.getSink(
    path="s3://hulopza-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Accelerometertrusted_node1676481954785",
)
Accelerometertrusted_node1676481954785.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="accelerometer_trusted"
)
Accelerometertrusted_node1676481954785.setFormat("json")
Accelerometertrusted_node1676481954785.writeFrame(Dropcolumns_node3)
job.commit()
