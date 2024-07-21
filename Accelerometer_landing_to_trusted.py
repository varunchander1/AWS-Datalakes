import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1721556990907 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1721556990907")

# Script generated for node Customer Trusted
CustomerTrusted_node1721556767299 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="customer_trustedtrusted", transformation_ctx="CustomerTrusted_node1721556767299")

# Script generated for node Join
Join_node1721557014874 = Join.apply(frame1=Accelerometerlanding_node1721556990907, frame2=CustomerTrusted_node1721556767299, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1721557014874")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1721557108524 = glueContext.write_dynamic_frame.from_options(frame=Join_node1721557014874, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-vc/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="Accelerometertrusted_node1721557108524")

job.commit()