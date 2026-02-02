# BOM ETL Job `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Cronjob**](#cronjob)

## Formål

Formålet med jobbet er at robotten atuomatisere aflæsning af byggesagsbehandlingstider og servicemål-opfyldelse på afgjorte sager i nøgletallene hentet i statistik-modulet fra (`https://sag.bygogmiljoe.dk/`) 

## Beskrivelse

Koden består af et ETL-job, der udfører følgende trin:

- Logger ind i BOM via Selenium med en Robot bruger(Robot_BOM)
-  Via Selenium hentes og aflæses byggebehandlingstider og servicemål-opfyldelse på afgjorte sager: (`Fra Dato`, `Kategori`, `Sagsbehandling`, `Servicemal Procent`,)
- Henter “Nøgletal” for to perioder:
   - **Månedlig**: fra 1. dag i forrige måned til 1. dag i indeværende måned
   - **Glidende gennemsnit**: fra 1. dag 12 måneder tilbage til 1. dag i indeværende måned
- Data bliver gemt i en Postgres DB for tabellerne (`bom_data_monthly`) og (`bom_data_glidende`)


**Dataflow:**
- Selenium RPA → Henter og aflæser nøgletal → Data gemmes i en Postgres DB i to forskellige tabeller

## Afhængigheder

Installér afhængigheder med:

```bash
pip install -r src/requirements.txt
```

:key: | **Miljøvariabler**

Byg og Miljø(BOM) login oplysning
- `BOM_USERNAME` Brugernavn til BOM login
- `BOM_PASSWORD` Adgangskode til BOM login

Postgres DB
- `BYGGESAGER_POSTGRES_DB_USER` Brugernavn til Byggesager Postgres DB
- `BYGGESAGER_POSTGRES_DB_PASS` Adgangskode til Byggesager Postgres DB
- `BYGGESAGER_POSTGRES_DB_HOST` Hostname til Byggesager Postgres DB
- `BYGGESAGER_POSTGRES_DB_DATABASE` Databasenavn til Byggesager Postgres DB
- `BYGGESAGER_POSTGRES_DB_PORT` Portnummer til Byggesager Postgres DB

## Cronjob

Cronjobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 den første dag på måneden 
- **Eksempler på kommende kørsler:**
  - 2025-09-01 00:00:00
  - 2025-10-01 00:00:00
  - 2025-11-01 00:00:00
  - 2025-12-01 00:00:00
  - 2025-01-01 0:00:00
- **Cron syntax:**  
  ```
  0 0 1 * *
  ```