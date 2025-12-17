import pandas as pd
import pyodbc
from pathlib import Path

# ===============================================================
#  CONFIGURATION DES CHEMINS
# ===============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
EXCEL_DIR = BASE_DIR / "data" / "excel"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
EXCEL_ORDER_OFFSET = 200000
EXCEL_ORDER_OFFSET = 200000


# ===============================================================
#  CONNEXION SQL SERVER
# ===============================================================
def get_sql_connection():
    """Retourne une connexion SQL Server ou None."""
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost\\SQLEXPRESS;"
            "DATABASE=Northwind;"
            "Trusted_Connection=yes;"
        )
        print("   ✔ Connexion SQL OK")
        return conn
    except Exception as e:
        print("   ❌ Erreur connexion SQL:", e)
        return None


# ===============================================================
#  LOAD EXCEL
# ===============================================================
def load_excel(table):
    """Charge un fichier Excel si disponible (accepte nom avec ou sans underscores)."""
    candidates = [
        EXCEL_DIR / f"{table}.xlsx",
        EXCEL_DIR / f"{table.replace(' ', '_')}.xlsx",
    ]
    for file in candidates:
        if file.exists():
            df = pd.read_excel(file)
            df.columns = [c.replace(" ", "") for c in df.columns]
            print(f"   Excel {table:<15}: {df.shape[0]:4} lignes, {df.shape[1]} colonnes")
            return df
    return None


# ===============================================================
#  LOAD SQL
# ===============================================================
def load_sql(table):
    """Charge une table SQL Server si disponible."""
    conn = get_sql_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql(f"SELECT * FROM [{table}]", conn)
        print(f"   SQL   {table:<15}: {df.shape[0]:4} lignes")
        return df
    except Exception:
        print(f"   ❌ Table SQL introuvable: {table}")
        return None


# ===============================================================
#  FUSION MULTI-SOURCES EXCEL + SQL - VERSION CORRIGÉE
# ===============================================================
def merge_sources(table, key, dedupe=True):
    """
    Fusionne Excel + SQL.
    Pour dedupe=False (Orders, Order Details), on GARDE LES DEUX sources séparément.
    """
    if isinstance(key, (list, tuple)):
        subset = list(key)
    else:
        subset = [key]

    df_excel = load_excel(table)
    df_sql = load_sql(table)

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
    if df_sql is None and df_excel is None:
        return None
    if df_sql is None:
        df = df_excel
    elif df_excel is None:
        df = df_sql
    else:
        df = pd.concat([df_excel, df_sql], ignore_index=True)

    if df is None:
        return None

    if dedupe:
        df = df.drop_duplicates(subset=subset, keep="first")
        df = df.dropna(subset=subset)
        df = df.drop(columns="_source", errors="ignore")

    return df.reset_index(drop=True)


# ===============================================================
#  ETAPE PRINCIPALE – BUILD STAR SCHEMA
# ===============================================================
def build_star_schema():

    print("\n2) TRANSFORMATION DES DONNÉES")
    print("-------------------------------")

    # 1) Chargement & fusion
    orders       = merge_sources("Orders",        "OrderID", dedupe=False)
    order_det    = merge_sources("Order Details", ["OrderID", "ProductID"], dedupe=False)
    customers    = merge_sources("Customers",     "CustomerID")
    products     = merge_sources("Products",      "ProductID")
    employees    = merge_sources("Employees",     "EmployeeID")
    shippers     = merge_sources("Shippers",      "ShipperID")
    categories   = merge_sources("Categories",    "CategoryID")

    print(f"\n   📊 APRÈS fusion:")
    print(f"      Orders: {len(orders)} lignes")
    print(f"      Order Details: {len(order_det)} lignes")

    # ===============================================================
    #  CLEAN DATES – REMPLIR LES DATES MANQUANTES
    # ===============================================================
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce")

    missing_dates = orders["OrderDate"].isna().sum()
    if missing_dates > 0:
        print(f"\n   ⚠ {missing_dates} lignes Orders avec OrderDate manquante")
        print(f"      → Remplacement par une date par défaut (1996-01-01)")
        orders["OrderDate"] = orders["OrderDate"].fillna(pd.Timestamp("1996-01-01"))

    # ===============================================================
    #  DIM TIME
    # ===============================================================
    dim_time = pd.DataFrame({
        "TimeKey": orders["OrderDate"].dt.strftime("%Y%m%d").astype(int),
        "date": orders["OrderDate"],
        "year": orders["OrderDate"].dt.year,
        "month": orders["OrderDate"].dt.month,
        "day": orders["OrderDate"].dt.day,
    }).drop_duplicates()

    # ===============================================================
    #  DIM CUSTOMERS
    # ===============================================================
    dim_customer = (
        customers[["CustomerID", "CompanyName", "City", "Country", "Phone"]]
        .rename(columns={
            "CustomerID": "CustomerKey",
            "CompanyName": "CustomerName",
            "City": "CustomerCity",
            "Country": "CustomerCountry"
        })
        .dropna(subset=["CustomerKey"])
        .drop_duplicates(subset=["CustomerKey"])
    )

    # ===============================================================
    #  DIM EMPLOYEES
    # ===============================================================
    employees["EmployeeFullName"] = employees["FirstName"] + " " + employees["LastName"]

    dim_employee = (
        employees[[
            "EmployeeID", "EmployeeFullName", "FirstName", "LastName",
            "Title", "City", "Country"
        ]]
        .rename(columns={"EmployeeID": "EmployeeKey"})
        .dropna(subset=["EmployeeKey"])
        .drop_duplicates(subset=["EmployeeKey"])
    )

    # ===============================================================
    #  DIM SHIPPERS
    # ===============================================================
    dim_shipper = (
        shippers[["ShipperID", "CompanyName", "Phone"]]
        .rename(columns={
            "ShipperID": "ShipperKey",
            "CompanyName": "ShipperName"
        })
        .assign(ShipperKey=lambda df: pd.to_numeric(df["ShipperKey"], errors="coerce").astype("Int64"))
        .dropna(subset=["ShipperKey"])
        .drop_duplicates(subset=["ShipperKey"])
    )

    # ===============================================================
    #  DIM CATEGORIES
    # ===============================================================
    dim_categories = (
        categories[["CategoryID", "CategoryName"]]
        .dropna(subset=["CategoryID"])
        .drop_duplicates(subset=["CategoryID"])
    )

    # ===============================================================
    #  FIX PRODUCTS
    # ===============================================================
    products["ProductID"] = pd.to_numeric(products["ProductID"], errors="coerce")

    invalid_products = products["ProductID"].isna().sum()
    if invalid_products > 0:
        print(f"\n   ⚠ {invalid_products} produits supprimés (ProductID invalide)")
        products = products.dropna(subset=["ProductID"])

    products = products.reset_index(drop=True)
    products = products.merge(dim_categories, on="CategoryID", how="left")
    products["CategoryName"] = products["CategoryName"].fillna("Unknown")

    dim_product = (
        pd.DataFrame({
            "ProductKey": products["ProductID"].astype(int),
            "ProductName": products["ProductName"],
            "UnitPrice": products["UnitPrice"],
            "CategoryName": products["CategoryName"],
            "Discontinued": products["Discontinued"].fillna(False).astype(bool)
        })
        .drop_duplicates(subset=["ProductKey"])
    )

    # ===============================================================
    #  FACT TABLE
    # ===============================================================
    print(f"\n   📊 CONSTRUCTION DE LA FACT TABLE:")
    print(f"      Order Details: {len(order_det)} lignes")
    print(f"      Orders       : {len(orders)} lignes")

    order_det["Discount"] = pd.to_numeric(order_det["Discount"], errors="coerce").fillna(0.0)
    order_det["Quantity"] = pd.to_numeric(order_det["Quantity"], errors="coerce").fillna(0.0)
    order_det["UnitPrice"] = pd.to_numeric(order_det["UnitPrice"], errors="coerce").fillna(0.0)
    order_det["LineTotal"] = order_det["UnitPrice"] * order_det["Quantity"] * (1 - order_det["Discount"])
    order_det["DetailCount"] = 1

    detail_summary = (
        order_det.groupby("OrderID", as_index=False)
            .agg(
                DetailCount=("DetailCount", "sum"),
                TotalQuantity=("Quantity", "sum"),
                AverageDiscount=("Discount", "mean"),
                TotalLineTotal=("LineTotal", "sum"),
            )
    )

    fact_sales = orders.drop(columns="_source", errors="ignore").merge(
        detail_summary,
        on="OrderID",
        how="left"
    )

    fact_sales["OrderDate"] = pd.to_datetime(fact_sales["OrderDate"], errors="coerce").fillna(pd.Timestamp("1996-01-01"))
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

    expected_orders = orders["OrderID"].nunique()
    gap = expected_orders - len(fact_sales)
    print(f"\n   📊 Fact_sales FINAL après agrégation: {len(fact_sales)} lignes")
    print(f"      → ATTENDU: {expected_orders} lignes (toutes les commandes fusionnées)")
    
    if gap == 0:
        print("      ✅ SUCCÈS: Toutes les commandes sont présentes!")
    else:
        print(f"      ⚠ ÉCART: {gap} commandes manquantes dans la fact table.")

    # ===============================================================
    #  EXPORT CSV
    # ===============================================================
    dim_time.to_csv(PROCESSED_DIR / "dim_time.csv", index=False)
    dim_customer.to_csv(PROCESSED_DIR / "dim_customer.csv", index=False)
    dim_product.to_csv(PROCESSED_DIR / "dim_product.csv", index=False)
    dim_employee.to_csv(PROCESSED_DIR / "dim_employee.csv", index=False)
    dim_shipper.to_csv(PROCESSED_DIR / "dim_shipper.csv", index=False)
    dim_categories.to_csv(PROCESSED_DIR / "dim_categories.csv", index=False)
    fact_sales.to_csv(PROCESSED_DIR / "fact_sales.csv", index=False)

    print("\n3) EXPORT DES TABLES (CSV) ✔ OK")
    print("============================================================")
    print("   🎉 ETL TERMINÉ AVEC SUCCÈS")
    print(f"   📊 RÉSUMÉ:")
    print(f"      • fact_sales: {len(fact_sales)} commandes")
    print(f"      • dim_customer: {len(dim_customer)} clients")
    print(f"      • dim_product: {len(dim_product)} produits")
    print(f"      • dim_employee: {len(dim_employee)} employés")
    print("============================================================")


# ===============================================================
#  RUN
# ===============================================================
if __name__ == "__main__":
    build_star_schema()
