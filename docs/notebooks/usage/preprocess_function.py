from datetime import datetime

def global_preprocess(df):
    sales_from_datetime = datetime(2025, 1, 3, 0, 0, 0)
    print(f"==> Global Preprocess")
    return df[df['datetime_sale'] >= sales_from_datetime]