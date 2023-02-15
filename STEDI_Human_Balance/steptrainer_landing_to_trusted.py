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

# Script generated for node step_trainer
step_trainer_node1676500165112 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/stepTrainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_node1676500165112",
)

# Script generated for node customer_curated
customer_curated_node1676500599769 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hulopza-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1676500599769",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1676500656034 = ApplyMapping.apply(
    frame=step_trainer_node1676500165112,
    mappings=[
        ("sensorReadingTime", "long", "`(right) sensorReadingTime`", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1676500656034",
)

# Script generated for node Join
Join_node1676500639610 = Join.apply(
    frame1=customer_curated_node1676500599769,
    frame2=RenamedkeysforJoin_node1676500656034,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node1676500639610",
)

# Script generated for node Drop Fields
DropFields_node1676500768063 = DropFields.apply(
    frame=Join_node1676500639610,
    paths=[
        "birthDay",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1676500768063",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1676500794440 = glueContext.getSink(
    path="s3://hulopza-lakehouse/stepTrainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1676500794440",
)
step_trainer_trusted_node1676500794440.setCatalogInfo(
    catalogDatabase="hlz", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1676500794440.setFormat("json")
step_trainer_trusted_node1676500794440.writeFrame(DropFields_node1676500768063)
job.commit()
