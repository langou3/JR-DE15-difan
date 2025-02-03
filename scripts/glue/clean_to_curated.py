import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'clean_zone_bucket',
    'curated_zone_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

cleaned_bucket = args['clean_zone_bucket']
curated_bucket = args['curated_zone_bucket']

paths = {
    "aisles": f"s3://{cleaned_bucket}/aisles/",
    "departments": f"s3://{cleaned_bucket}/departments/",
    "products": f"s3://{cleaned_bucket}/products/",
    "orders": f"s3://{cleaned_bucket}/orders/",
    "order_products": f"s3://{cleaned_bucket}/order_products/",
}

dynamic_frames = {}

for table, path in paths.items():

    # Load data from S3
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format="parquet",
        format_options={
            "withHeader": True,
            "separator": ","},
    ).toDF()

    # Save cleaned DataFrame.
    dynamic_frames[table] = dynamic_frame

try:
    order_products_prior = dynamic_frames["order_products"]\
        .join(dynamic_frames["orders"], "order_id")\
        .filter(col("eval_set") == "prior")\
        .withColumn("is_weekend", F.when(col("order_dow").isin(0, 6), 1).otherwise(0))

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(order_products_prior, glueContext,"order_products_prior").coalesce(1),
        connection_type="s3",
        connection_options={"path": f"s3://{curated_bucket}/order_products_prior/"},
        format="parquet",
        format_options={
            "withHeader": True,
        }
    )   
    

    user_features = order_products_prior.groupBy("user_id")\
        .agg(
            F.max("order_number").alias("user_orders"),                             # Max of order_number
            F.count("*").alias("user_total_products"),
            F.sum("days_since_prior_order").alias("user_period"),                   # Sum of days_since_prior_order
            F.avg("days_since_prior_order").alias("user_mean_days_since_prior"),    # Avg of days_since_prior_order
            F.countDistinct("product_id").alias("user_distinct_products"),          # Distinct count of product_id per user_id
            (F.sum(F.when(col("reordered") == 1, 1).otherwise(0)) /                 # Sum for reordered = 1
            F.sum(F.when(col("order_number") > 1, 1).otherwise(0)).cast("double")).alias("user_reorder_ratio"),
            F.round(F.avg("is_weekend"), 2).alias("weekend_order_ratio"),           # Proportion of weekend orders
            F.coalesce(F.sum(F.when(col("reordered") == 1, 1).otherwise(0)), F.lit(0)).alias("total_reorders")\
            )\
            .withColumn(
                "user_average_basket",
                F.round(F.col("user_total_products") / F.col("user_orders"), 2)  # Avg products per order           
            )\
            .orderBy("user_id")
    
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(user_features, glueContext,"user_features").coalesce(1),
        connection_type="s3",
        connection_options={"path": f"s3://{curated_bucket}/user_features/"},
        format="parquet",
        format_options={
            "withHeader": True,
        }
    )   
    
    up_features = order_products_prior.groupBy("user_id", "product_id")\
        .agg(
            F.count("*").alias("up_orders"),
            F.sum("reordered").alias("up_reorders"),  # Total times the product was reordered
            F.min("order_number").alias("up_first_order"),  # First order number the user bought the product
            F.max("order_number").alias("up_last_order"),  # Last order number the user bought the product
            F.round(F.avg("add_to_cart_order"), 2).alias("up_avg_add_to_cart_order"),  # Avg position in cart
            F.round(F.avg("days_since_prior_order"), 2).alias("up_avg_time_since_last_order")  # Avg time since prior order
        )
    
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(up_features, glueContext,"up_features").coalesce(1),
        connection_type="s3",
        connection_options={"path":f"s3://{curated_bucket}/up_features/"},
        format="parquet",
        format_options={
            "withHeader": True,
        }
    )       

    
    window_spec = Window.partitionBy("user_id", "product_id").orderBy("order_number")
    prd_features = order_products_prior.withColumn(
        "product_seq_time", 
        F.rank().over(window_spec))\
    .groupBy("product_id").agg(
        F.count("*").alias("prod_orders"),                                          # Total orders for the product
        F.sum("reordered").alias("prod_reorders"),                           # Total reorders for the product
        F.sum(F.when(col("product_seq_time") == 1, 1).otherwise(0)).alias("prod_first_orders"),
        F.sum(F.when(col("product_seq_time") == 2, 1).otherwise(0)).alias("prod_second_orders"),
        F.round(F.avg("add_to_cart_order"), 2).alias("prod_avg_cart_position"),  # Average cart position
        F.round(F.avg("reordered"), 2).alias("prod_reorder_rate"),  # Reorder rate (popularity)
        ).withColumn(
            "prod_reorder_probability",
            F.round(F.col("prod_second_orders") / F.col("prod_first_orders"), 2)
        ).withColumn(
            "prod_reorder_times",
            F.round(1 + F.col("prod_reorders") / F.col("prod_first_orders"), 2)
        )


    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(prd_features, glueContext,"prd_features").coalesce(1),
        connection_type="s3",
        connection_options={"path":f"s3://{curated_bucket}/product_features/"},
        format="parquet",
        format_options={
            "withHeader": True,
        }
    )    

    # creating dataframes from existing athena catelog
    order_products_prior = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", connection_options = {"paths": [f"s3://{curated_bucket}/order_products_prior/"]}).toDF()
    up_features = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", connection_options = {"paths": [f"s3://{curated_bucket}/up_features/"]}).toDF()
    prd_features = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", connection_options = {"paths": [f"s3://{curated_bucket}/product_features/"]}).toDF()
    user_features = glueContext.create_dynamic_frame_from_options(connection_type = "parquet", connection_options = {"paths": [f"s3://{curated_bucket}/user_features/"]}).toDF()
    

    """### Tranform categorical variables using OneHotEncoding"""
    # # merged_data = order_products_prior.join(dynamic_frames["products"], on="product_id", how="inner")

    # # ==========================
    # # OneHotEncoding for merged_data
    # # ==========================
    # # Define the columns for OneHotEncoding
    # columns_to_encode = ["aisle_id", "department_id"]

    # # Create stages for the pipeline
    # indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in columns_to_encode]
    # encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded") for col in columns_to_encode]

    # # Combine indexers and encoders into a single pipeline
    # pipeline_stages = indexers + encoders
    # merged_data_pipeline = Pipeline(stages=pipeline_stages)

    # # Fit and transform the data
    # merged_data = merged_data_pipeline.fit(merged_data).transform(merged_data)

    # # Ensure these features are included in the final output without altering the aggregation
    # product_features = prd_features.join(
    #     merged_data.select("product_id", "department_id_encoded", "aisle_id_encoded").distinct(),
    #     on="product_id",
    #     how="left"
    # )

    # Drop intermediate columns
    product_features = prd_features.drop("prod_reorders", "prod_first_orders", "prod_second_orders")
    user_features = user_features.drop('total_reorders')

    # train and test orders and order products
    ml_datasets_orders = dynamic_frames["orders"][dynamic_frames["orders"]['eval_set'] != "prior"][['user_id', 'order_id', 'eval_set']]
    ml_datasets_order_products = ml_datasets_orders.join(dynamic_frames["order_products"].select('order_id', "product_id", "reordered"), on=['order_id'], how='inner') # Perform the inner join

    #Filtering the train and test data from features
    user_features = user_features.join(ml_datasets_orders.select("user_id"), on="user_id", how="inner")

    up_features = ml_datasets_order_products.join(up_features, on=['user_id','product_id'], how="left")
    up_features = up_features.fillna(0)

    product_features = ml_datasets_order_products.select("product_id", "order_id").join(product_features, on="product_id", how="left")
    product_features = product_features.fillna(0)
        
    """### Join up_features, product_features, and user_features together. Add the product_name column"""
    up_features = up_features.join(user_features, on="user_id", how="left")

    up_features = up_features\
        .withColumn(
            "up_order_rate", # Rate of orders for the product
            F.round(F.col("up_orders") / F.col("user_orders"), 2))\
        .withColumn(
            "up_orders_since_last_order", # Orders since the last time this product was bought
            F.col("user_orders") - F.col("up_last_order"))\
        .withColumn(
            "up_order_rate_since_first_order", # Rate since first order
            F.round(F.col("up_orders") / (F.col("user_orders") - F.col("up_first_order") + 1), 2))
    

    # Step 1: Join `up_features` and `product_features`
    up_product_features = up_features.join(
        product_features,
        on=["product_id", "order_id"],
        how="inner"  # Use "left" to retain all rows from `up_features`
    )


    # Step 3: Add the product_name column
    final_result = up_product_features.join(
        dynamic_frames["products"],
        on="product_id",
        how="inner"
    )
    final_result = final_result.drop("aisle_id","department_id", "order_id")

    # train and test orders
    ml_datasets = dynamic_frames["orders"][dynamic_frames["orders"]['eval_set'] != "prior"][['user_id', 'order_id', 'eval_set']]
    ml_datasets = ml_datasets.join(dynamic_frames["order_products"].select('order_id', "product_id", "reordered"), on=['order_id'], how='inner') # Perform the inner join

    ml_datasets = ml_datasets.join(final_result, on=['user_id','product_id'], how='left')
    ml_datasets = ml_datasets.drop("aisle_id","department_id", "order_id")

    training_data = final_result.filter(col("eval_set") == "train")
    test_data = final_result.filter(col("eval_set") == "test")


    # Write the resulting DataFrame to S3 in CSV format
    training_data.repartition(1).write.mode('append').parquet(f"s3://{curated_bucket}/ml_dataset/training_data")
    test_data.repartition(1).write.mode('append').parquet(f"s3://{curated_bucket}/ml_dataset/test_data")

    job.commit()
except Exception as e:
    print(f"Error in ETL job: {str(e)}")
    raise e
