# scripts/test_etl.py
# ===============================
# Validation rapide des CSV ETL
# ===============================

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

TABLES = {
    "dim_time.csv": {
        "required": ["TimeKey", "date", "year", "month", "day"],
    },
    "dim_customer.csv": {
        "required": ["CustomerKey", "CustomerName", "CustomerCity", "CustomerCountry", "Phone"],
    },
    "dim_product.csv": {
        "required": ["ProductKey", "ProductName", "UnitPrice", "CategoryName", "Discontinued"],
    },
    "dim_employee.csv": {
        "required": ["EmployeeKey", "EmployeeFullName", "FirstName", "LastName", "Title", "City", "Country"],
    },
    "dim_shipper.csv": {
        "required": ["ShipperKey", "ShipperName", "Phone"],
    },
    "fact_sales.csv": {
        "required": [
            "OrderKey",
            "TimeKey",
            "CustomerKey",
            "EmployeeKey",
            "ShipperKey",
            "DetailCount",
            "TotalQuantity",
            "AverageDiscount",
            "TotalLineTotal",
        ],
    },
}


def main():
    print("=" * 60)
    print(" TEST DE VALIDATION DES CSV")
    print("=" * 60)
    print()

    all_ok = True

    for filename, info in TABLES.items():
        path = PROCESSED_DIR / filename
        print(filename)
        print("-" * 60)

        if not path.exists():
            print("   ❌ Fichier introuvable : ", path)
            all_ok = False
            print()
            continue

        df = pd.read_csv(path)
        required_cols = info["required"]
        missing = [c for c in required_cols if c not in df.columns]

        if missing:
            print("   ❌ Colonnes manquantes : ", missing)
            all_ok = False
        else:
            print("   ✅ Toutes les colonnes requises sont présentes")
            print(f"   Lignes   : {len(df)}")
            print(f"   Colonnes : {len(df.columns)}")
            print("   Aperçu (3 premières lignes) :")
            preview = df.head(3)
            for _, row in preview.iterrows():
                for col in required_cols[:5]:  # afficher max 5 colonnes importantes
                    print(f"      • {col}: {row[col]}")
                print("      ---")
        print()

    print("=" * 60)
    if all_ok:
        print(" ✅ TOUS LES TESTS SONT PASSÉS")
    else:
        print(" ❌ CERTAINS TESTS ONT ÉCHOUÉ – vérifier l'ETL")
    print("=" * 60)


if __name__ == "__main__":
    main()
