from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType as PySparkIntegerType,
    StringType as PySparkStringType,
    FloatType as PySparkFloatType,
    BooleanType as PySparkBooleanType,
    DecimalType as PySparkDecimalType,
)
from pyspark.sql.functions import col as spark_col, lit as spark_lit

from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    StructType as SnowStructType,
    StructField as SnowStructField,
    IntegerType as SnowIntegerType,
    StringType as SnowStringType,
    FloatType as SnowFloatType,
    BooleanType as SnowBooleanType,
    DecimalType as SnowDecimalType,
)
from snowflake.snowpark.functions import col as snow_col

print("=" * 80)
print("Converting PySpark DataFrame to Snowpark DataFrame")
print("=" * 80)

# ============================================================================
# Step 1: Create PySpark Session and DataFrame
# ============================================================================
print("\n" + "=" * 80)
print("Step 1: Creating PySpark DataFrame")
print("=" * 80)

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark to Snowpark Conversion") \
    .master("local[*]") \
    .getOrCreate()

print("✅ PySpark session created")

# Define PySpark schema
pyspark_schema = StructType([
    StructField("PRODUCT_ID", PySparkIntegerType(), False),
    StructField("PRODUCT_NAME", PySparkStringType(), False),
    StructField("CATEGORY", PySparkStringType(), True),
    StructField("PRICE", PySparkDecimalType(10, 2), False),
    StructField("IN_STOCK", PySparkBooleanType(), False),
    StructField("RATING", PySparkFloatType(), True),
])

# Create sample data
data = [
    (1, "Laptop", "Electronics", 999.99, True, 4.5),
    (2, "Mouse", "Electronics", 29.99, True, 4.2),
    (3, "Keyboard", "Electronics", 79.99, False, 4.0),
    (4, "Monitor", "Electronics", 299.99, True, 4.7),
    (5, "Headphones", "Electronics", 149.99, True, 4.3),
    (6, "Desk Chair", "Furniture", 249.99, True, 4.6),
    (7, "Standing Desk", "Furniture", 599.99, False, 4.8),
]

# Create PySpark DataFrame
pyspark_df = spark.createDataFrame(data, schema=pyspark_schema)

print("✅ PySpark DataFrame created")
print(f"   Columns: {pyspark_df.columns}")
print(f"   Row count: {pyspark_df.count()}")
print(f"\n   Schema:")
pyspark_df.printSchema()

print("\n📊 PySpark DataFrame contents:")
pyspark_df.show()

# ============================================================================
# Step 2: Convert PySpark DataFrame to Pandas (intermediate step)
# ============================================================================
print("\n" + "=" * 80)
print("Step 2: Converting PySpark DataFrame to Pandas")
print("=" * 80)

# Convert to Pandas
pandas_df = pyspark_df.toPandas()

print("✅ Converted to Pandas DataFrame")
print(f"   Shape: {pandas_df.shape}")
print(f"   Columns: {list(pandas_df.columns)}")
print(f"   Dtypes:\n{pandas_df.dtypes}")

print("\n📊 Pandas DataFrame:")
print(pandas_df)

# ============================================================================
# Step 3: Create Snowpark Session
# ============================================================================
print("\n" + "=" * 80)
print("Step 3: Creating Snowpark Session")
print("=" * 80)

# Connection parameters for real Snowflake (commented out)
# connection_parameters = {
#     "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
#     "user": os.environ.get("SNOWFLAKE_USER"),
#     "password": os.environ.get("SNOWFLAKE_PASSWORD"),
#     "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
#     "database": os.environ.get("SNOWFLAKE_DATABASE"),
#     "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
# }
# snow_session = Session.builder.configs(connection_parameters).create()

# Create local testing session
snow_session = Session.builder.config('local_testing', True).create()

print("✅ Snowpark session created")
print(f"   Session ID: {snow_session.session_id}")

# ============================================================================
# Step 4: Convert Pandas DataFrame to Snowpark DataFrame
# ============================================================================
print("\n" + "=" * 80)
print("Step 4: Converting Pandas to Snowpark DataFrame")
print("=" * 80)

# Method 1: Direct conversion from Pandas to Snowpark
# Define Snowpark schema (matching PySpark schema)
snowpark_schema = SnowStructType([
    SnowStructField("PRODUCT_ID", SnowIntegerType(), nullable=False),
    SnowStructField("PRODUCT_NAME", SnowStringType(), nullable=False),
    SnowStructField("CATEGORY", SnowStringType(), nullable=True),
    SnowStructField("PRICE", SnowDecimalType(10, 2), nullable=False),
    SnowStructField("IN_STOCK", SnowBooleanType(), nullable=False),
    SnowStructField("RATING", SnowFloatType(), nullable=True),
])

# Convert Pandas to list of rows for Snowpark
snowpark_data = pandas_df.values.tolist()

# Create Snowpark DataFrame
snowpark_df = snow_session.create_dataframe(snowpark_data, schema=snowpark_schema)

print("✅ Snowpark DataFrame created")
print(f"   Columns: {snowpark_df.columns}")
print(f"   Row count: {snowpark_df.count()}")
print(f"\n   Schema:")
for field in snowpark_df.schema.fields:
    nullable = "NULL" if field.nullable else "NOT NULL"
    print(f"      {field.name:20} {str(field.datatype):25} {nullable}")

print("\n📊 Snowpark DataFrame contents:")
snowpark_df.show()

# ============================================================================
# Step 5: Verify the Conversion - Compare Data
# ============================================================================
print("\n" + "=" * 80)
print("Step 5: Verifying Conversion")
print("=" * 80)

print("\n🔍 Comparison:")
print(f"   PySpark row count:  {pyspark_df.count()}")
print(f"   Pandas row count:   {len(pandas_df)}")
print(f"   Snowpark row count: {snowpark_df.count()}")

print("\n🔍 Column comparison:")
print(f"   PySpark columns:  {pyspark_df.columns}")
print(f"   Snowpark columns: {snowpark_df.columns}")

print("\n🔍 Schema comparison:")
print("\n   PySpark Schema:")
for field in pyspark_df.schema.fields:
    print(f"      {field.name:20} {str(field.dataType):25} nullable={field.nullable}")

print("\n   Snowpark Schema:")
for field in snowpark_df.schema.fields:
    print(f"      {field.name:20} {str(field.datatype):25} nullable={field.nullable}")

# ============================================================================
# Step 6: Perform Operations on Snowpark DataFrame
# ============================================================================
print("\n" + "=" * 80)
print("Step 6: Operations on Converted Snowpark DataFrame")
print("=" * 80)

# Filter operation
print("\n🔍 Filter: Products in stock")
in_stock = snowpark_df.filter(snow_col("IN_STOCK") == True)
print(f"   Found {in_stock.count()} products in stock")
in_stock.select("PRODUCT_ID", "PRODUCT_NAME", "PRICE").show()

# Aggregate operation
print("\n🔍 Aggregate: Average price by category")
from snowflake.snowpark.functions import avg, count

avg_price = snowpark_df.group_by("CATEGORY").agg([
    count("PRODUCT_ID").alias("COUNT"),
    avg("PRICE").alias("AVG_PRICE"),
])
avg_price.show()

# Transform operation
print("\n🔍 Transform: Apply 10% discount")
discounted = snowpark_df.with_column(
    "DISCOUNTED_PRICE",
    snow_col("PRICE") * 0.9
)
discounted.select("PRODUCT_ID", "PRODUCT_NAME", "PRICE", "DISCOUNTED_PRICE").show()

# ============================================================================
# Step 7: Alternative Method - Using createDataFrame without schema
# ============================================================================
print("\n" + "=" * 80)
print("Step 7: Alternative Method - Auto-infer Schema")
print("=" * 80)

# Create Snowpark DataFrame from Pandas without explicit schema
snowpark_df_auto = snow_session.create_dataframe(pandas_df)

print("✅ Snowpark DataFrame created with auto-inferred schema")
print(f"   Columns: {snowpark_df_auto.columns}")
print(f"\n   Auto-inferred Schema:")
for field in snowpark_df_auto.schema.fields:
    print(f"      {field.name:20} {str(field.datatype):25}")

print("\n📊 Auto-schema DataFrame contents:")
snowpark_df_auto.show()

# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 80)
print("✅ Conversion Summary")
print("=" * 80)
print("""
Conversion Path:
  PySpark DataFrame → Pandas DataFrame → Snowpark DataFrame

Key Steps:
  1. Create PySpark DataFrame with schema
  2. Convert to Pandas using .toPandas()
  3. Convert to Snowpark using session.create_dataframe()
  
Methods:
  • Explicit schema: Better type control
  • Auto-infer schema: Simpler but less control
  
Notes:
  - Pandas is the intermediate format
  - Schema mapping may require adjustment
  - Decimal types need explicit precision/scale
  - For large datasets, consider saving to file/table
""")

# Cleanup
print("\n🧹 Cleanup...")
spark.stop()
snow_session.close()
print("✅ Sessions closed")

print("\n" + "=" * 80)
print("✅ All operations completed successfully!")
print("=" * 80)
