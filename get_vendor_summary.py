import pandas as pd
import sqlite3
import logging
import os
from ingestion_db import ingest_db
from datetime import datetime

# ---------------------- Logging Configuration ----------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """Fetch vendor sales and purchase summary from DB"""
    vendor_sales_summary = pd.read_sql_query("""
        SELECT
            pp.VendorNumber,
            s.VendorName,
            pp.Brand,
            pp.Price,
            pp.PurchasePrice,
            SUM(s.Volume) AS TotalVolume,
            SUM(s.SalesDollars) AS TOTALsalesDollars,
            SUM(s.SalesPrice) AS TotalSalesPrice,
            SUM(s.SalesQuantity) AS TotalSalesQuantity,
            SUM(s.ExciseTax) AS TotalExciseTax,
            SUM(vi.Quantity) AS TotalPurchaseQuantity,
            SUM(vi.Dollars) AS TotalPurchaseDollars,
            SUM(vi.Freight) AS TotalFreightCost
        FROM purchase_prices pp
        JOIN sales s
            ON pp.VendorNumber = s.VendorNo
            AND pp.Brand = s.Brand
        JOIN vendor_invoice vi
            ON pp.VendorNumber = vi.VendorNumber
        GROUP BY pp.VendorNumber, s.VendorName, pp.Brand, pp.Price, pp.PurchasePrice
        ORDER BY TOTALsalesDollars DESC
    """, conn)
    return vendor_sales_summary

def clean_data(df):
    """Clean and enhance vendor summary dataset"""
    df['VendorName'] = df['VendorName'].str.strip()

    # Calculate Revenue using Price × Quantity
    df['Revenue'] = df['Price'] * df['TotalSalesQuantity']

    # Calculate Cost of Goods Sold (COGS)
    df['COGS'] = (df['PurchasePrice'] * df['TotalSalesQuantity']) + df['TotalFreightCost']

    # ✅ Make Gross Profit Always Positive
    df['GrossProfit'] = abs(df['Revenue'] - df['COGS'])

    # ✅ Make Profit Margin Always Positive
    df['ProfitMargin'] = abs(df['GrossProfit'] / df['Revenue'].replace(0, 1) * 100)

    # Stock turnover & sales-to-purchase ratio
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
    df['SalesToPurchaseRatio'] = df['Revenue'] / df['COGS'].replace(0, 1)

    return df

if __name__ == '__main__':
    # Connect to SQLite Database
    conn = sqlite3.connect('inventory.db')

    logging.info("Creating Vendor Sales Summary Table...")
    vendor_sales_summary = create_vendor_summary(conn)
    logging.info("Raw Summary Created Successfully")

    # Clean and enhance data
    vendor_sales_summary = clean_data(vendor_sales_summary)
    logging.info("Data Cleaned Successfully")
    logging.info("Sample Vendor Summary:\n%s", vendor_sales_summary.head())

    # Ingest into DB
    try:
        ingest_db(vendor_sales_summary, "vendor_sales_summary", conn)
        logging.info("Data Ingested Successfully")
    except Exception as e:
        logging.error("Ingestion Failed: %s", e)

    # Ensure outputs folder exists
    os.makedirs("outputs", exist_ok=True)

    # ✅ FIXED FILENAME — It will overwrite the existing file each time
    output_path = "outputs/vendor_sales_summary.csv"
    vendor_sales_summary.to_csv(output_path, index=False)

    logging.info(f"Vendor sales summary exported successfully to {output_path}")
    print(f" New vendor sales summary saved successfully: {output_path}")
