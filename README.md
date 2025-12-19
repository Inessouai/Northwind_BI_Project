# Northwind Business Intelligence

## Apercu du projet
Ce depot propose une chaine BI de bout en bout construite sur la base Northwind (Access 2012 ou SQL Server). Les scripts fournissent l ingestion des donnees, leur nettoyage, la validation automatisee puis un tableau de bord Streamlit pour suivre les indicateurs commerciaux (CA, commandes, clients, panier moyen, repartition par pays et categories).

## Fonctionnalites cles
- Pipeline ETL multi-sources (Northwind.accdb ou SQL Server) avec normalisation des dimensions
- Controle qualite systematique via `scripts/test_etl.py`
- Tableau de bord Streamlit interactif (`scripts/dashboard_northwind.py`) alimente par Plotly
- Scripts separes pour Access et SQL Server afin de s adapter a l environnement cible
- Export des donnees nettoyees dans `data/processed` pour un usage externe

## Structure du depot
- `scripts/` : ETL, tests et dashboard
- `data/excel/` : feuilles Excel extraites d Access
- `data/processed/` : CSV generes (dimensions, fait)
- `figures/`, `rapport/`, `video/` : communication et ressources livrables
- `Northwind.sql` : script SQL Server optionnel pour reconstruire la base

## Pre-requis
- Python 3.10 ou plus (developpement teste sous 3.13)
- Microsoft Access Database Engine + ODBC Driver 17 for SQL Server
- Fichier `data/Northwind 2012.accdb` ou une instance SQL Server avec la base Northwind
- Acces en lecture/ecriture au dossier `data/processed` pour generer les CSV

## Installation des dependances
```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```
Les dependances sont listees dans `requirements.txt`. Vous pouvez utiliser `pip install -r requirements.txt --upgrade` pour mettre a jour les versions si besoin.

## Dependances Python
| Package | Version | Raison principale |
| --- | --- | --- |
| pandas | 2.1.4 | Manipulation, jointures et enrichissement des jeux de donnees |
| pyodbc | 5.0.1 | Connexion Access/SQL Server via ODBC |
| streamlit | 1.29.0 | Application web interactive pour le dashboard |
| plotly | 5.18.0 | Visualisations dynamiques integrees au dashboard |

## Executer le pipeline
### 1. Extraction et transformation Access
```powershell
python scripts/etl_northwind.py
```
Le script lit `data/Northwind 2012.accdb` et les fichiers Excel dans `data/excel/`, aligne les schemas, construit les dimensions (`dim_customer`, `dim_product`, `dim_employee`, `dim_shipper`, `dim_time`) puis la table de faits `fact_sales`. Les sorties sont placees dans `data/processed/`.

### 2. Extraction SQL Server (optionnel)
```powershell
python scripts/etl_northwind_sqlserver.py
```
Mettez a jour la fonction `get_sql_connection()` si votre instance n utilise pas `localhost\SQLEXPRESS` ou une base nommee `Northwind`. Ce flux peut fonctionner seul ou en parallele des exports Access.

### 3. Tests de qualite des donnees
```powershell
python scripts/test_etl.py
```
Le rapport de tests confirme la presence de chaque CSV, la liste des colonnes obligatoires et affiche un apercu des cinq premiers champs pour les trois premieres lignes. Le message `TOUS LES TESTS SONT PASSES` signifie que les fichiers sont exploitables.

### 4. Tableau de bord BI
```powershell
streamlit run scripts/dashboard_northwind.py
```
Streamlit affiche l URL locale (par defaut `http://localhost:8504`). Les filtres (annee, pays, categorie) pilotent les indicateurs clefs: chiffre d affaires, commandes, clients actifs, panier moyen, repartition geographique et les top produits/clients.

## Jeux de donnees generes
- `data/processed/dim_customer.csv`
- `data/processed/dim_employee.csv`
- `data/processed/dim_product.csv`
- `data/processed/dim_shipper.csv`
- `data/processed/dim_time.csv`
- `data/processed/fact_sales.csv`


## Auteur
Projet realise par Ines Souai (ING3 Securite Informatique).
