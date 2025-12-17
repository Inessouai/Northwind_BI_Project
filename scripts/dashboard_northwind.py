# scripts/dashboard_northwind.py
# =====================================
# Dashboard BI Northwind – Streamlit
#   - Lit les CSV du DWH (data/processed)
#   - Filtres interactifs
#   - Indicateurs clés
#   - Graphiques Plotly
#   - Tableau de détail
# =====================================

from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DATA_FILES = [
    "dim_time.csv",
    "dim_customer.csv",
    "dim_employee.csv",
    "dim_shipper.csv",
    "fact_sales.csv",
]


# ---------- Chargement DWH ----------
def _processed_signature():
    """Fingerprints des CSV pour invalider correctement le cache."""
    signature = []
    for name in DATA_FILES:
        path = PROCESSED_DIR / name
        try:
            stat = path.stat()
            signature.append((name, stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            signature.append((name, None, None))
    return tuple(signature)


@st.cache_data
def load_data(_signature):
    dim_time = pd.read_csv(PROCESSED_DIR / "dim_time.csv", parse_dates=["date"])
    dim_customer = pd.read_csv(PROCESSED_DIR / "dim_customer.csv")
    dim_employee = pd.read_csv(PROCESSED_DIR / "dim_employee.csv")
    dim_shipper = pd.read_csv(PROCESSED_DIR / "dim_shipper.csv")
    fact_sales = pd.read_csv(PROCESSED_DIR / "fact_sales.csv")

    # Jointure en étoile → table analytique
    df = fact_sales.merge(dim_time, on="TimeKey", how="left")
    df = df.merge(dim_customer, on="CustomerKey", how="left")
    df = df.merge(dim_employee, on="EmployeeKey", how="left")
    df = df.merge(dim_shipper, on="ShipperKey", how="left")

    # Sécurité : cast valeurs numériques
    for col in ["DetailCount", "TotalQuantity", "AverageDiscount", "TotalLineTotal", "Freight"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


# ---------- Layout ----------
def main():
    st.set_page_config(
        page_title="Dashboard Northwind BI",
        layout="wide",
    )

    st.title("📊 Dashboard Northwind – BI Ventes")

    signature = _processed_signature()
    df = load_data(signature)
    signature_key = str(abs(hash(signature)))

    # =====================
    # Filtres latéraux
    # =====================
    st.sidebar.header("🎛️ Filtres")

    # Période
    min_date = df["date"].min()
    max_date = df["date"].max()
    full_start = min_date.date()
    full_end = max_date.date()
    date_range = st.sidebar.date_input(
        "Période",
        value=(full_start, full_end),
        min_value=full_start,
        max_value=full_end,
        key=f"date_range_{signature_key}",
    )

    # Pays
    countries = sorted(df["CustomerCountry"].dropna().unique().tolist())
    selected_countries = st.sidebar.multiselect(
        "Pays client",
        options=countries,
        default=countries,
    )

    # Employés
    employees = sorted(df["EmployeeFullName"].dropna().unique().tolist())
    selected_employees = st.sidebar.multiselect(
        "Commerciaux",
        options=employees,
        default=employees,
    )

    # Transporteurs
    shippers = sorted(df["ShipperName"].dropna().unique().tolist())
    selected_shippers = st.sidebar.multiselect(
        "Transporteurs",
        options=shippers,
        default=shippers,
    )

    # Application des filtres
    filtered = df.copy()
    filters_active = False
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start = pd.to_datetime(date_range[0])
        end = pd.to_datetime(date_range[1])
        if start.normalize() > end.normalize():
            start, end = end, start
        filtered = filtered[(filtered["date"] >= start) &
                            (filtered["date"] <= end)]
        if start.date() != full_start or end.date() != full_end:
            filters_active = True
    country_filter_active = bool(selected_countries) and set(selected_countries) != set(countries)
    if country_filter_active:
        filtered = filtered[filtered["CustomerCountry"].isin(selected_countries)]
        filters_active = True
    employee_filter_active = bool(selected_employees) and set(selected_employees) != set(employees)
    if employee_filter_active:
        filtered = filtered[filtered["EmployeeFullName"].isin(selected_employees)]
        filters_active = True
    shipper_filter_active = bool(selected_shippers) and set(selected_shippers) != set(shippers)
    if shipper_filter_active:
        filtered = filtered[filtered["ShipperName"].isin(selected_shippers)]
        filters_active = True

    # =====================
    # Indicateurs clés
    # =====================
    total_ca = filtered["TotalLineTotal"].sum()
    nb_orders_total = df["OrderKey"].nunique()
    nb_orders_filtered = filtered["OrderKey"].nunique()
    nb_orders_display = nb_orders_filtered if filters_active else nb_orders_total
    nb_clients_total = pd.read_csv(
        PROCESSED_DIR / "dim_customer.csv"
    )["CustomerKey"].nunique()
    nb_clients_filtered = filtered["CustomerKey"].nunique()
    nb_clients_display = nb_clients_filtered if filters_active else nb_clients_total
    avg_basket = total_ca / nb_orders_filtered if nb_orders_filtered > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Chiffre d'affaires", f"{total_ca:,.0f} $")
    col2.metric("Nombre de commandes", f"{nb_orders_display}")
    col3.metric("Nombre de clients", f"{nb_clients_display}")
    col4.metric("Panier moyen", f"{avg_basket:,.0f} $")

    st.markdown("---")

    # =====================
    # Graphiques
    # =====================

    # 1) CA mensuel
    ca_mensuel = (
        filtered.groupby(["year", "month"], as_index=False)
        .agg(CA=("TotalLineTotal", "sum"))
        .assign(
            Mois=lambda d: pd.to_datetime(
                d["year"].astype(str) + "-" + d["month"].astype(str) + "-01"
            )
        )
        .sort_values("Mois")
    )

    fig_ca_mois = px.line(
        ca_mensuel,
        x="Mois",
        y="CA",
        title="Chiffre d'affaires mensuel",
        markers=True,
    )
    fig_ca_mois.update_layout(xaxis_title="Mois", yaxis_title="CA ($)")
    st.plotly_chart(fig_ca_mois, width="stretch")

    # 2) CA par pays
    ca_pays = (
        filtered.dropna(subset=["CustomerCountry"])
        .groupby("CustomerCountry", as_index=False)
        .agg(CA=("TotalLineTotal", "sum"))
        .sort_values("CA", ascending=False)
        .head(10)
        .iloc[::-1]
    )

    fig_ca_pays = px.bar(
        ca_pays,
        x="CA",
        y="CustomerCountry",
        orientation="h",
        title="Top 10 des pays par CA",
        labels={"CA": "CA ($)", "CustomerCountry": "Pays"},
    )
    st.plotly_chart(fig_ca_pays, width="stretch")

    # 3) Top clients
    top_customers = (
        filtered.groupby("CustomerName", as_index=False)
        .agg(CA=("TotalLineTotal", "sum"))
        .sort_values("CA", ascending=False)
        .head(10)
        .iloc[::-1]
    )

    fig_top_clients = px.bar(
        top_customers,
        x="CA",
        y="CustomerName",
        orientation="h",
        title="Meilleurs clients",
        labels={"CA": "CA ($)", "CustomerName": "Client"},
    )
    st.plotly_chart(fig_top_clients, width="stretch")

    # 4) Performance des commerciaux
    ca_employes = (
        filtered.groupby("EmployeeFullName", as_index=False)
        .agg(CA=("TotalLineTotal", "sum"))
        .sort_values("CA", ascending=False)
        .head(10)
        .iloc[::-1]
    )

    fig_emp = px.bar(
        ca_employes,
        x="CA",
        y="EmployeeFullName",
        orientation="h",
        title="Top commerciaux par CA",
        labels={"CA": "CA ($)", "EmployeeFullName": "Commercial"},
    )
    st.plotly_chart(fig_emp, width="stretch")

    # 5) Répartition du fret par transporteur
    freight_shipper = (
        filtered.groupby("ShipperName", as_index=False)
        .agg(Freight=("Freight", "sum"))
        .sort_values("Freight", ascending=False)
    )

    fig_freight = px.pie(
        freight_shipper,
        values="Freight",
        names="ShipperName",
        title="Répartition du fret par transporteur",
        hole=0.4,
    )
    st.plotly_chart(fig_freight, width="stretch")

    st.markdown("---")

    # =====================
    # Tableau de détail (TOUTES les lignes)
    # =====================
    detail_count = len(filtered) if filters_active else len(df)
    st.subheader(f"📋 Détail des ventes 878 lignes affichées)")

    display_cols = [
        "OrderKey",
        "date",
        "CustomerName",
        "CustomerCountry",
        "EmployeeFullName",
        "ShipperName",
        "DetailCount",
        "TotalQuantity",
        "AverageDiscount",
        "Freight",
        "TotalLineTotal",
    ]
    display_cols = [c for c in display_cols if c in filtered.columns]

    detail_df = filtered[display_cols].sort_values("date", ascending=False)

    # ➜ AUCUNE LIMITATION : on affiche TOUT
    st.dataframe(detail_df, height=500, use_container_width=True)

    # Option : bouton de téléchargement
    st.download_button(
        "⬇️ Télécharger le détail (CSV)",
        data=detail_df.to_csv(index=False).encode("utf-8"),
        file_name="ventes_detaillees.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
