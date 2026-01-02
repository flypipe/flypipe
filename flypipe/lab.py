from snowflake.snowpark import Session
import os

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Integer, Float, Date, Boolean, Decimal

# Connection parameters for real Snowflake connection (commented out)
# connection_parameters = {
#   "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
#   "user": os.environ.get("SNOWFLAKE_USER"),
#   "password": os.environ.get("SNOWFLAKE_PASSWORD"),
# }

# Create local testing session
session = Session.builder.config('local_testing', True).create()

print("=" * 80)
print("Testing Snowflake Snowpark Integration with Flypipe")
print("=" * 80)


# Node 1: Source node that creates initial data
@node(
    type="snowpark",
    description="Source node that creates product data",
    session_context=True,
    output=Schema(
        Column("PRODUCT_ID", Integer(), "Unique product identifier"),
        Column("PRODUCT_NAME", String(), "Name of the product"),
        Column("PRICE", Float(), "Product price"),
        Column("IN_STOCK", Boolean(), "Whether product is in stock"),
    )
)
def products(session):
    """Create a dataframe with product information"""
    data = [
        [1, "Laptop", 999.99, True],
        [2, "Mouse", 29.99, True],
        [3, "Keyboard", 79.99, False],
        [4, "Monitor", 299.99, True],
        [5, "Headphones", 149.99, True],
    ]
    columns = ["PRODUCT_ID", "PRODUCT_NAME", "PRICE", "IN_STOCK"]
    return session.create_dataframe(data, columns)


# Node 2: Transformation node that processes product data
@node(
    type="snowpark",
    description="Calculate discounted prices for products",
    dependencies=[products.select("PRODUCT_ID", "PRODUCT_NAME", "PRICE", "IN_STOCK").alias("products")],
    session_context=True,
    output=Schema(
        Column("PRODUCT_ID", Integer(), "Unique product identifier"),
        Column("PRODUCT_NAME", String(), "Name of the product"),
        Column("ORIGINAL_PRICE", Float(), "Original product price"),
        Column("DISCOUNT_RATE", Float(), "Discount rate applied"),
        Column("DISCOUNTED_PRICE", Decimal(precision=10, scale=2), "Price after discount"),
        Column("IN_STOCK", Boolean(), "Whether product is in stock"),
    )
)
def discounted_products(session, products):
    """Apply discount to products and calculate new prices"""
    import snowflake.snowpark.functions as F
    
    # Apply 10% discount
    df = products.with_column("ORIGINAL_PRICE", products["PRICE"])
    df = df.with_column("DISCOUNT_RATE", F.lit(0.10))
    df = df.with_column("DISCOUNTED_PRICE", df["PRICE"] * (1 - df["DISCOUNT_RATE"]))
    
    return df.select(
        "PRODUCT_ID",
        "PRODUCT_NAME", 
        "ORIGINAL_PRICE",
        "DISCOUNT_RATE",
        "DISCOUNTED_PRICE",
        "IN_STOCK"
    )


print("\n" + "=" * 80)
print("Running Node 1: products")
print("=" * 80)
result1 = products.run(session)
print("\n✅ Products DataFrame:")
print(f"   Columns: {result1.columns}")
print(f"   Schema: {result1.schema}")
print(f"   Row count: {result1.count()}")
result1.show()

print("\n" + "=" * 80)
print("Running Node 2: discounted_products (with dependency)")
print("=" * 80)
result2 = discounted_products.run(session)
print("\n✅ Discounted Products DataFrame:")
print(f"   Columns: {result2.columns}")
print(f"   Schema: {result2.schema}")
print(f"   Row count: {result2.count()}")
result2.show()

print("\n" + "=" * 80)
print("✅ All tests completed successfully!")
print("=" * 80)
print("\nVerifying type casting:")
print(f"  - PRODUCT_ID type: {result2.schema['PRODUCT_ID'].datatype}")
print(f"  - PRODUCT_NAME type: {result2.schema['PRODUCT_NAME'].datatype}")
print(f"  - ORIGINAL_PRICE type: {result2.schema['ORIGINAL_PRICE'].datatype}")
print(f"  - DISCOUNT_RATE type: {result2.schema['DISCOUNT_RATE'].datatype}")
print(f"  - DISCOUNTED_PRICE type: {result2.schema['DISCOUNTED_PRICE'].datatype}")
print(f"  - IN_STOCK type: {result2.schema['IN_STOCK'].datatype}")
print("=" * 80)