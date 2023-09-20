import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, regexp_extract, when


# Script generated for node Custom Transform
def MyTransform3(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_expiry_data = df.withColumn(
        "doc_issue_date",
        when(col("doc_issue_date") == "DoC not issued", None).otherwise(col("doc_issue_date")),
    )
    dyf_replaced = DynamicFrame.fromDF(df_expiry_data, glueContext, "replaceValues1")
    return DynamicFrameCollection({"CustomTransform3": dyf_replaced}, glueContext)


# Script generated for node Custom Transform
def MyTransform0(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()

    extracted_eiv = when(
        col("technical_efficiency").isNotNull(),
        when(
            col("technical_efficiency") != "Not Applicable",
            when(col("technical_efficiency").contains("EIV"), "EIV")
            .when(col("technical_efficiency").contains("EEDI"), "EEDI")
            .otherwise(None),
        ),
    ).otherwise(None)

    df_with_eiv = df.withColumn("technical_efficiency_type", extracted_eiv)

    dyf_replaced = DynamicFrame.fromDF(df_with_eiv, glueContext, "extractValues")
    return DynamicFrameCollection({"CustomTransform0": dyf_replaced}, glueContext)


# Script generated for node Custom Transform
def MyTransform4(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_new = df.na.drop(subset=["doc_issue_date", "port_of_registry", "technical_efficiency"])

    dyf_replaced = DynamicFrame.fromDF(df_new, glueContext, "dropNulls")
    return DynamicFrameCollection({"CustomTransform4": dyf_replaced}, glueContext)


# Script generated for node Custom Transform
def MyTransform2(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_expiry_data = df.withColumn(
        "doc_expiry_date",
        when(col("doc_expiry_date") == "DoC not issued", None).otherwise(col("doc_expiry_date")),
    )
    dyf_replaced = DynamicFrame.fromDF(df_expiry_data, glueContext, "replaceValues")
    return DynamicFrameCollection({"CustomTransform2": dyf_replaced}, glueContext)


# Script generated for node Custom Transform
def MyTransform1(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()

    extracted_eiv = when(
        col("technical_efficiency").isNotNull(),
        when(
            col("technical_efficiency") != "Not Applicable",
            regexp_extract(col("technical_efficiency"), r"\d+(\.\d+)?", 0),
        ),
    ).otherwise(None)

    df_with_eiv = df.withColumn("technical_efficiency_value", extracted_eiv)

    dyf_replaced = DynamicFrame.fromDF(df_with_eiv, glueContext, "extractValues")
    return DynamicFrameCollection({"CustomTransform1": dyf_replaced}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1692955271176 = glueContext.create_dynamic_frame.from_catalog(
    database="raw_emissions_converted",
    table_name="ship_emissions",
    transformation_ctx="AWSGlueDataCatalog_node1692955271176",
)

# Script generated for node Custom Transform
CustomTransform_node1692955286909 = MyTransform0(
    glueContext,
    DynamicFrameCollection(
        {"AWSGlueDataCatalog_node1692955271176": AWSGlueDataCatalog_node1692955271176},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1692955380585 = SelectFromCollection.apply(
    dfc=CustomTransform_node1692955286909,
    key=list(CustomTransform_node1692955286909.keys())[0],
    transformation_ctx="SelectFromCollection_node1692955380585",
)

# Script generated for node Custom Transform
CustomTransform_node1692955389840 = MyTransform1(
    glueContext,
    DynamicFrameCollection(
        {"SelectFromCollection_node1692955380585": SelectFromCollection_node1692955380585},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1692955430249 = SelectFromCollection.apply(
    dfc=CustomTransform_node1692955389840,
    key=list(CustomTransform_node1692955389840.keys())[0],
    transformation_ctx="SelectFromCollection_node1692955430249",
)

# Script generated for node Custom Transform
CustomTransform_node1692955439276 = MyTransform2(
    glueContext,
    DynamicFrameCollection(
        {"SelectFromCollection_node1692955430249": SelectFromCollection_node1692955430249},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1692955510042 = SelectFromCollection.apply(
    dfc=CustomTransform_node1692955439276,
    key=list(CustomTransform_node1692955439276.keys())[0],
    transformation_ctx="SelectFromCollection_node1692955510042",
)

# Script generated for node Custom Transform
CustomTransform_node1692955518357 = MyTransform3(
    glueContext,
    DynamicFrameCollection(
        {"SelectFromCollection_node1692955510042": SelectFromCollection_node1692955510042},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1692955543346 = SelectFromCollection.apply(
    dfc=CustomTransform_node1692955518357,
    key=list(CustomTransform_node1692955518357.keys())[0],
    transformation_ctx="SelectFromCollection_node1692955543346",
)

# Script generated for node Custom Transform
CustomTransform_node1692955550484 = MyTransform4(
    glueContext,
    DynamicFrameCollection(
        {"SelectFromCollection_node1692955543346": SelectFromCollection_node1692955543346},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1692955577712 = SelectFromCollection.apply(
    dfc=CustomTransform_node1692955550484,
    key=list(CustomTransform_node1692955550484.keys())[0],
    transformation_ctx="SelectFromCollection_node1692955577712",
)

# Script generated for node Amazon S3
AmazonS3_node1692955600629 = glueContext.getSink(
    path="s3://eu-marv-ship-emissions/clean/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1692955600629",
)
AmazonS3_node1692955600629.setCatalogInfo(
    catalogDatabase="final_clean_emissions", catalogTableName="emissions_all"
)
AmazonS3_node1692955600629.setFormat("glueparquet")
AmazonS3_node1692955600629.writeFrame(SelectFromCollection_node1692955577712)
job.commit()
