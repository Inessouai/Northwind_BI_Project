import pyodbc

def main():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=NorthwindBI;"
        "Trusted_Connection=yes;"
    )

    print("Connexion à SQL Server…")
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT TOP 5 name FROM sys.tables;")
        rows = cur.fetchall()
        print("✅ Connexion SQL OK, quelques tables :")
        for r in rows:
            print(" -", r[0])

if __name__ == "__main__":
    main()
