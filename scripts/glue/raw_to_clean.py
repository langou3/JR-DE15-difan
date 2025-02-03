import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_zone_bucket',
    'clean_zone_bucket'
])

raw_bucket = args['raw_zone_bucket']
cleaned_bucket = args['clean_zone_bucket']
invalid_bucket = ""

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

paths = {
    "aisles": f"s3://{raw_bucket}/data/aisles.csv",
    "departments": f"s3://{raw_bucket}/data/departments.csv",
    "products": f"s3://{raw_bucket}/data/products.csv",
    "orders": f"s3://{raw_bucket}/data/orders.csv",
    "order_products": f"s3://{raw_bucket}/data/order_products.csv",
}

#Manually define each table's schema.
schemas = {
    "aisles": {"aisle_id": "bigint", "aisle": "string"},
    "departments": {"department_id": "bigint", "department": "string"},
    "products": {
        "product_id": "bigint",
        "product_name": "string",
        "aisle_id": "bigint",
        "department_id": "bigint",
    },
    "orders": {
        "order_id": "bigint",
        "user_id": "bigint",
        "eval_set": "string",
        "order_number": "bigint",
        "order_dow": "bigint",
        "order_hour_of_day": "bigint",
        "days_since_prior_order": "double",
    },
    "order_products": {
        "order_id": "bigint",
        "product_id": "bigint",
        "add_to_cart_order": "bigint",
        "reordered": "bigint",
    },
}

# Data Cleaning and Validation Function
def clean_and_validate(dynamic_frame, schema, table_name):
    #convert to DataFrame
    df = dynamic_frame.toDF()

    # Handle NULL values
    if table_name == "orders":
        df = df.fillna({"days_since_prior_order": -1})

    # Validate Schema and save invalid rows
    for col_name, col_type in schema.items():
        invalid_rows = df.filter((df[col_name].isNull()) | (~df[col_name].cast(col_type).isNotNull()))
        if invalid_rows.count() > 0:
            invalid_rows.write.parquet(f"{invalid_bucket}{table_name}_invalid_{col_name}/", mode="overwrite")
        df = df.filter(df[col_name].cast(col_type).isNotNull())

    # Special Validation Rules
    if table_name == "orders":
        invalid_dow = df.filter((df["order_dow"] < 0) | (df["order_dow"] > 6))
        if invalid_dow.count() > 0:
            invalid_dow.write.parquet(f"{invalid_bucket}{table_name}_invalid_order_dow/", mode="overwrite")
        df = df.filter((df["order_dow"] >= 0) & (df["order_dow"] <= 6))

        invalid_hour = df.filter((df["order_hour_of_day"] < 0) | (df["order_hour_of_day"] > 23))
        if invalid_hour.count() > 0:
            invalid_hour.write.parquet(f"{invalid_bucket}{table_name}_invalid_order_hour/", mode="overwrite")
        df = df.filter((df["order_hour_of_day"] >= 0) & (df["order_hour_of_day"] <= 23))

    if table_name == "order_products":
        invalid_reordered = df.filter(~df["reordered"].isin([0, 1]))
        if invalid_reordered.count() > 0:
            invalid_reordered.write.parquet(f"{invalid_bucket}{table_name}_invalid_reordered/", mode="overwrite")
            print(f"Found invalid reordered values in {table_name}.")
        df = df.filter(df["reordered"].isin([0, 1]))

    return df

# Cross-Table Validation Function
def cross_table_validation(dynamic_frames):
    #convert to dataframe
    aisles_df = dynamic_frames["aisles"]
    departments_df = dynamic_frames["departments"]
    products_df = dynamic_frames["products"]
    orders_df = dynamic_frames["orders"]
    order_products_df = dynamic_frames["order_products"]

    # Validate products against aisles
    invalid_aisle_ids = products_df.join(aisles_df, "aisle_id", "left_anti")
    if invalid_aisle_ids.count() > 0:
        invalid_aisle_ids.write.parquet(f"{invalid_bucket}invalid_aisle_ids/", mode="overwrite")

    # Validate products against departments
    invalid_department_ids = products_df.join(departments_df, "department_id", "left_anti")
    if invalid_department_ids.count() > 0:
        invalid_department_ids.write.parquet(f"{invalid_bucket}invalid_department_ids/", mode="overwrite")

    # Validate order_products against products
    invalid_product_ids = order_products_df.join(products_df, "product_id", "left_anti")
    if invalid_product_ids.count() > 0:
        invalid_product_ids.write.parquet(f"{invalid_bucket}invalid_product_ids/", mode="overwrite")

    # Validate order_products against orders
    invalid_order_ids = order_products_df.join(orders_df, "order_id", "left_anti")
    if invalid_order_ids.count() > 0:
        invalid_order_ids.write.parquet(f"{invalid_bucket}invalid_order_ids/", mode="overwrite")


try:
    # Data Processing
    dynamic_frames = {}
    for table, path in paths.items():
    
        # Load data from S3
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="csv",
            format_options={
                "withHeader": True,
                "separator": ","},
        )
    
        # Clean and validate
        cleaned_df = clean_and_validate(dynamic_frame, schemas[table], table)
    
        # # Partition logic
        # if table == "orders":
        #     # Force 1 partition
        #     cleaned_df = cleaned_df.coalesce(1)
        # elif table == "order_products":
        #     # Force 5 partitions
        #     cleaned_df = cleaned_df.repartition(5)
    
        # Convert back to DynamicFrame
        cleaned_frame = DynamicFrame.fromDF(cleaned_df, glueContext, f"cleaned_{table}")
    
        # Write cleaned data to S3
        glueContext.write_dynamic_frame.from_options(
            frame=cleaned_frame,
            connection_type="s3",
            connection_options={"path": f"s3://{cleaned_bucket}/{table}/"},
            format="parquet",
        )
    
        # Save cleaned DataFrame for cross-table validation
        dynamic_frames[table] = cleaned_df
    
    # Cross-table validation
    cross_table_validation(dynamic_frames)
    # Commit Glue Job
    job.commit()
    
except Exception as e:
    print(f"Error in ETL job: {str(e)}")
    raise e
