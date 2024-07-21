import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1721558742787 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="customer_trustedtrusted", transformation_ctx="CustomerTrusted_node1721558742787")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721558760948 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1721558760948")

# Script generated for node Join
SqlQuery1176 = '''
select * 
from customer_trusted
where email in (select distinct user 
                  from accelerometer_trusted)
and sharewithresearchasofdate is not null

'''
Join_node1721558967985 = sparkSqlQuery(glueContext, query = SqlQuery1176, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1721558760948, "customer_trusted":CustomerTrusted_node1721558742787}, transformation_ctx = "Join_node1721558967985")

# Script generated for node Amazon S3
AmazonS3_node1721559128231 = glueContext.write_dynamic_frame.from_options(frame=Join_node1721558967985, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-vc/customer/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1721559128231")

job.commit()