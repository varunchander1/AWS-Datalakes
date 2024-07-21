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

# Script generated for node Customer landing
Customerlanding_node1721553411887 = glueContext.create_dynamic_frame.from_catalog(database="stedi-vc", table_name="customer_landing", transformation_ctx="Customerlanding_node1721553411887")

# Script generated for node Share with Research
SqlQuery1271 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SharewithResearch_node1721553696428 = sparkSqlQuery(glueContext, query = SqlQuery1271, mapping = {"myDataSource":Customerlanding_node1721553411887}, transformation_ctx = "SharewithResearch_node1721553696428")

# Script generated for node Customer Trusted
CustomerTrusted_node1721553808355 = glueContext.getSink(path="s3://stedi-lake-house-vc/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1721553808355")
CustomerTrusted_node1721553808355.setCatalogInfo(catalogDatabase="stedi-vc",catalogTableName="customer_trusted")
CustomerTrusted_node1721553808355.setFormat("json")
CustomerTrusted_node1721553808355.writeFrame(SharewithResearch_node1721553696428)
job.commit()