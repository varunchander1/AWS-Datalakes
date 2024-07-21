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

# Script generated for node Step-trainer-trusted
Steptrainertrusted_node1721564790513 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="step_trainer_trusted", transformation_ctx="Steptrainertrusted_node1721564790513")

# Script generated for node Accelerometer-trusted
Accelerometertrusted_node1721564811903 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1721564811903")

# Script generated for node Customer_curated
Customer_curated_node1721565049269 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="customer_curated", transformation_ctx="Customer_curated_node1721565049269")

# Script generated for node Join
SqlQuery1317 = '''
select a.*,b.user,b.timestamp,b.x,b.y,b.z
from step_trainer_trusted as a inner join
accelerometer_trusted as b
on a.sensorreadingtime = b.timestamp
where a.serialnumber in 
            (select distinct serialnumber 
               from customer_curated)



'''
Join_node1721564835400 = sparkSqlQuery(glueContext, query = SqlQuery1317, mapping = {"accelerometer_trusted":Accelerometertrusted_node1721564811903, "step_trainer_trusted":Steptrainertrusted_node1721564790513, "customer_curated":Customer_curated_node1721565049269}, transformation_ctx = "Join_node1721564835400")

# Script generated for node machine_learning_curated
machine_learning_curated_node1721565340056 = glueContext.write_dynamic_frame.from_options(frame=Join_node1721564835400, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-vc/machine_learning/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="machine_learning_curated_node1721565340056")

job.commit()