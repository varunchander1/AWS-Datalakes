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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1721563549638 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1721563549638")

# Script generated for node Customer Curated
CustomerCurated_node1721563582481 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="customer_curated", transformation_ctx="CustomerCurated_node1721563582481")

# Script generated for node SQL Query
SqlQuery1299 = '''
select * from Step_trainer_landing
where serialnumber in (select distinct serialnumber 
                         from customer_curated)

'''
SQLQuery_node1721563611547 = sparkSqlQuery(glueContext, query = SqlQuery1299, mapping = {"Customer_curated":CustomerCurated_node1721563582481, "Step_trainer_landing":StepTrainerLanding_node1721563549638}, transformation_ctx = "SQLQuery_node1721563611547")

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1721563723039 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1721563611547, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-vc/step-trainer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="Step_trainer_trusted_node1721563723039")

job.commit()