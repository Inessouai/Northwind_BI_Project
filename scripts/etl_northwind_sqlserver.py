
import pyodbc
import pandas as pd
from pathlib import Path

# Dossiers
BASE_DIR = Path(__file__).resolve().parent.parent  
DATA_DIR = BASE_DIR / "data"
EXCEL_DIR = DATA_DIR / "excel"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
EXCEL_ORDER_OFFSET = 200000

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=Northwind;"
    "Trusted_Connection=yes;"
)


def get_connection():
    return pyodbc.connect(CONN_STR)


def load_table(name: str, conn) -> pd.DataFrame:
    query = f"SELECT * FROM [{name}]"
    return pd.read_sql(query, conn)

def load_excel_table(name: str) -> pd.DataFrame | None:
    candidates = [
        EXCEL_DIR / f"{name}.xlsx",
        EXCEL_DIR / f"{name.replace(' ', '_')}.xlsx",
    ]
    for file in candidates:
        if file.exists():
            df = pd.read_excel(file)
            df.columns = [c.replace(" ", "") for c in df.columns]
            print(f"   Excel {name:<15}: {len(df)} lignes")
            return df
    return None

def merge_with_excel(sql_df: pd.DataFrame, table: str, key_columns, dedupe=True):
    subset = list(key_columns) if isinstance(key_columns, (list, tuple)) else [key_columns]
    df_excel = load_excel_table(table)

    def _adjust_excel_ids(df):
        df = df.copy()
        name = table.lower().replace("_", " ").strip()
        if name in {"orders", "order details"}:
            if "OrderID" not in df.columns:
                raise KeyError(f"Colonne OrderID introuvable dans le fichier Excel pour la table {table}")
            df["OrderID"] = pd.to_numeric(df["OrderID"], errors="coerce")
            df = df.dropna(subset=["OrderID"])
            df["OrderID"] = df["OrderID"].astype(int) + EXCEL_ORDER_OFFSET
        return df

    if df_excel is not None:
        df_excel = _adjust_excel_ids(df_excel)

    if df_excel is None and sql_df is None:
        return None
    if sql_df is None:
        combined = df_excel
    elif df_excel is None:
        combined = sql_df
    else:
        combined = pd.concat([df_excel, sql_df], ignore_index=True)

    if combined is not None and dedupe:
        combined = combined.drop_duplicates(subset=subset, keep="first")
        combined = combined.dropna(subset=subset)

    return combined.reset_index(drop=True) if combined is not None else None


def build_etl():
    print("🔌 Connexion à SQL Server...")
    with get_connection() as conn:
        print("✅ Connecté.")

        print("📥 Chargement des tables source...")
        orders = load_table("Orders", conn)
        order_details = load_table("Order Details", conn)
        customers = load_table("Customers", conn)
        products = load_table("Products", conn)
        employees = load_table("Employees", conn)
        shippers = load_table("Shippers", conn)
        categories = load_table("Categories", conn)

    # Fusion Excel + SQL (si fichiers Excel présents)
    orders = merge_with_excel(orders, "Orders", "OrderID")
    order_details = merge_with_excel(order_details, "Order Details", ["OrderID", "ProductID"])
    customers = merge_with_excel(customers, "Customers", "CustomerID")
    products = merge_with_excel(products, "Products", "ProductID")
    employees = merge_with_excel(employees, "Employees", "EmployeeID")
    shippers = merge_with_excel(shippers, "Shippers", "ShipperID")
    categories = merge_with_excel(categories, "Categories", "CategoryID")

    # ---------- Dimension Temps ----------
    print("🧱 Construction DimTime...")
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])
    dim_time = (
        orders[["OrderDate"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"OrderDate": "date"})
        .reset_index(drop=True)
    )

    dim_time["TimeKey"] = dim_time["date"].dt.strftime("%Y%m%d").astype(int)
    dim_time["year"] = dim_time["date"].dt.year
    dim_time["month"] = dim_time["date"].dt.month
    dim_time["day"] = dim_time["date"].dt.day
    dim_time["year_month"] = dim_time["date"].dt.strftime("%Y-%m")

    dim_time = dim_time[
        ["TimeKey", "date", "year", "month", "day", "year_month"]
    ].sort_values("date")

    # ---------- Dimension Client ----------
    print("🧱 Construction DimCustomer...")
    dim_customer = customers.rename(
        columns={
            "CustomerID": "CustomerKey",
            "CompanyName": "CustomerName",
            "Country": "CustomerCountry",
            "City": "CustomerCity",
        }
    )

    dim_customer = (
        dim_customer[
            [
                "CustomerKey",
                "CustomerName",
                "ContactName",
                "ContactTitle",
                "CustomerCity",
                "CustomerCountry",
                "Phone",
            ]
        ]
        .dropna(subset=["CustomerKey"])
        .drop_duplicates(subset=["CustomerKey"])
    )

    # ---------- Dimension Produit ----------
    print("🧱 Construction DimProduct...")
    dim_product = products.merge(
        categories[["CategoryID", "CategoryName"]],
        on="CategoryID",
        how="left",
    )

    dim_product = dim_product.rename(
        columns={
            "ProductID": "ProductKey",
            "ProductName": "ProductName",
        }
    )

    dim_product = (
        dim_product[
            [
                "ProductKey",
                "ProductName",
                "QuantityPerUnit",
                "UnitPrice",
                "UnitsInStock",
                "UnitsOnOrder",
                "ReorderLevel",
                "Discontinued",
                "CategoryName",
            ]
        ]
        .dropna(subset=["ProductKey"])
        .drop_duplicates(subset=["ProductKey"])
    )

    # ---------- Dimension Employe ----------
    print("🧱 Construction DimEmployee...")
    employees["EmployeeFullName"] = employees["FirstName"] + " " + employees["LastName"]

    dim_employee = (
        employees[
            [
                "EmployeeID",
                "EmployeeFullName",
                "FirstName",
                "LastName",
                "Title",
                "City",
                "Country",
            ]
        ]
        .rename(columns={"EmployeeID": "EmployeeKey"})
        .dropna(subset=["EmployeeKey"])
        .drop_duplicates(subset=["EmployeeKey"])
    )

    # ---------- Dimension Transporteur ----------
    print("🧱 Construction DimShipper...")
    dim_shipper = shippers.rename(
        columns={
            "ShipperID": "ShipperKey",
            "CompanyName": "ShipperName",
        }
    )

    dim_shipper = (
        dim_shipper[["ShipperKey", "ShipperName", "Phone"]]
        .assign(ShipperKey=lambda df: pd.to_numeric(df["ShipperKey"], errors="coerce").astype("Int64"))
        .dropna(subset=["ShipperKey"])
        .drop_duplicates(subset=["ShipperKey"])
    )

    # ---------- Table de Faits ----------
    print("🧮 Construction FactSales...")
    order_details["Discount"] = pd.to_numeric(order_details["Discount"], errors="coerce").fillna(0.0)
    order_details["Quantity"] = pd.to_numeric(order_details["Quantity"], errors="coerce").fillna(0.0)
    order_details["UnitPrice"] = pd.to_numeric(order_details["UnitPrice"], errors="coerce").fillna(0.0)
    order_details["LineTotal"] = (
        order_details["UnitPrice"] * order_details["Quantity"] * (1 - order_details["Discount"])
    )
    order_details["DetailCount"] = 1

    detail_summary = (
        order_details.groupby("OrderID", as_index=False)
            .agg(
                DetailCount=("DetailCount", "sum"),
                TotalQuantity=("Quantity", "sum"),
                AverageDiscount=("Discount", "mean"),
                TotalLineTotal=("LineTotal", "sum"),
            )
    )

    fact_sales = orders.merge(detail_summary, on="OrderID", how="left")
    fact_sales["OrderDate"] = pd.to_datetime(fact_sales["OrderDate"])
    fact_sales["TimeKey"] = fact_sales["OrderDate"].dt.strftime("%Y%m%d").astype(int)
    fact_sales["DetailCount"] = fact_sales["DetailCount"].fillna(0).astype(int)
    fact_sales["TotalQuantity"] = fact_sales["TotalQuantity"].fillna(0.0)
    fact_sales["AverageDiscount"] = fact_sales["AverageDiscount"].fillna(0.0)
    fact_sales["TotalLineTotal"] = fact_sales["TotalLineTotal"].fillna(0.0)
    fact_sales["Freight"] = pd.to_numeric(fact_sales["Freight"], errors="coerce").fillna(0.0)
    fact_sales = fact_sales.rename(
        columns={
            "OrderID": "OrderKey",
            "CustomerID": "CustomerKey",
            "EmployeeID": "EmployeeKey",
            "ShipVia": "ShipperKey",
        }
    )
    fact_sales["CustomerKey"] = fact_sales["CustomerKey"].astype(str).str.strip()
    for col in ["EmployeeKey", "ShipperKey"]:
        fact_sales[col] = (
            pd.to_numeric(fact_sales[col], errors="coerce")
            .astype("Int64")
        )

    # ---------- Sauvegarde ----------
    print("💾 Sauvegarde des CSV dans:", PROCESSED_DIR)
    dim_time.to_csv(PROCESSED_DIR / "dim_time.csv", index=False)
    dim_customer.to_csv(PROCESSED_DIR / "dim_customer.csv", index=False)
    dim_product.to_csv(PROCESSED_DIR / "dim_product.csv", index=False)
    dim_employee.to_csv(PROCESSED_DIR / "dim_employee.csv", index=False)
    dim_shipper.to_csv(PROCESSED_DIR / "dim_shipper.csv", index=False)
    fact_sales.to_csv(PROCESSED_DIR / "fact_sales.csv", index=False)

    print("✅ ETL terminé.")


if __name__ == "__main__":
    build_etl()
