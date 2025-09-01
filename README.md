# Vendor-Performance-Data-Analytics-SQL-Python-Power-BI-
End‑to‑end analytics project to evaluate vendor performance, pricing efficiency, inventory turnover, and profitability, using SQL for data prep, Python for EDA and modeling, and Power BI for an executive dashboard.

Key outcomes
Identified brands for promotional/pricing actions (low sales, high margins).

Quantified top‑10 vendor purchase concentration at 65.69%, signaling diversification risk.

Demonstrated bulk purchasing reduces unit cost by ~72% (to ~10.78 per unit for large orders).

Compared profit margins for high vs low performers; low performers showed higher margins but lower volumes.

Repository structure
data/

inventory.db — SQLite database with raw/aggregated tables.

vendor_sales_summary_clean (1).xls — curated summary for BI.

ingestion_db.log — data ingestion logs.

src/

ingestion_db.py — loads/updates inventory.db, logging enabled.

get_vendor_summary.py — creates aggregated vendor/product metrics.

notebooks/

vendor performance analysis.ipynb — end‑to‑end analysis narrative.

bi/

powerbi-dashboard/ — .pbix and export images of the dashboard.

docs/

report/ — PDF/Markdown project report (this repo).



Business problem
Retail/wholesale companies must avoid losses from inefficient pricing, poor inventory turnover, and vendor dependency; goals include spotting underperforming brands, top vendors by sales/profit, bulk purchasing effects, inventory turnover, and profitability variance

Data pipeline
Ingestion: Python scripts pull and append transaction tables to SQLite (inventory.db) with structured logging.

Transformation: SQL aggregates purchase/sales, costs, taxes, freight, and computes KPIs (gross profit, margin, stock turnover, sales‑to‑purchase ratio).

Curation: get_vendor_summary.py outputs vendor_sales_summary_clean (1).xls for BI.

Analytics: Jupyter EDA for summary statistics, outliers, correlations, and filtering rules.

BI: Power BI dashboard with KPI cards, concentration donut, vendor/brand rankings, and scatter for brand performance.


Key metrics
GrossProfit, ProfitMargin, StockTurnover, SalesToPurchaseRatio, FreightCost.

Negative/zero checks; filtered out GrossProfit ≤ 0, ProfitMargin ≤ 0, TotalSalesQuantity = 0 for reliable insights.

Methods and insights
Outlier review: high SD in purchase/actual prices indicates premium SKUs; freight variability suggests logistics differences.

Correlations: weak between purchase price and sales/profit; strong between purchase quantity and sales quantity; negative between margin and sales price.

Vendor concentration: top 10 vendors = 65.69% of purchases; diversify suppliers.

Bulk pricing: large orders achieve ~10.78 unit cost (~72% lower vs small orders).

Power BI dashboard
KPIs: Total Sales, Total Purchase, Gross Profit, Profit Margin, Unsold Capital.

Visuals: Purchase Contribution %, Top Vendors by Sales, Top Brands by Sales, Low‑performing vendors/brands, Brand scatter vs total sales.










Exploratory Data Analysis.ipynb — distributions, outliers, correlations.

logging.ipynb — reproducible logging tests.
