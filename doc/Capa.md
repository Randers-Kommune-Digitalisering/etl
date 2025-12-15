# Capa ETL Job `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Cronjob**](#cronjob)

## Formål

Formålet med jobbet er at hente computer specifikke data fra CAPA CMS databasen, samt data fra SFTP og indsætte resultatet i en Postgres DB.

## Beskrivelse

Kode består af et ETL-job, der udfører følgende trin:

- Laver SQL queries til CAPA CMS databasen som henter følgende data (`Unitname`, `Serienummer`, `PrimaryUser`, `Afdeling`, `PrimaryFullName`)
- (`DeviceLicense`) bliver hentet fra SFTP
- SQL queries og data fra CAPA CMS databasen og data fra SFTP'en bliver indsættet i en ny Postgres DB

**Dataflow:**
- Data fra CAPA CMS → Data fra SFTP → Gemmes i en Postgres DB

## Afhængigheder

Installér afhængigheder med:

```bash
pip install -r src/requirements.txt
```

:key: | **Miljøvariabler**

CAPA CMS Database gennem VPN

- `CAPA_CMS_DB_USER` Brugernavn til CAPA CMS MSSQL DB
- `CAPA_CMS_DB_PASS` Adgangskode til CAPA CMS MSSQL DB
- `CAPA_CMS_DB_HOST` Hostname til CAPA CMS MSSQL DB
- `CAPA_CMS_DB_DATABASE` Databasenavn til CAPA CMS MSSQL DB
- `CAPA_CMS_DB_PORT` Portnummer til CAPA CMS MSSQL DB

SFTP til Asset(DeviceLicense)
- `ASSET_SFTP_HOST` Hostname til Asset SFTP
- `ASSET_SFTP_USER` Burgernavn til Asset SFTP
- `ASSET_SFTP_PASS` Adgangskode til Asset SFTP
- `ASSET_SFTP_FILE_PATH` Mappe directory path til filen med DeviceLicense data 

Postgres DB
- `ASSET_DB_USER` Brugernavn til Asset Postgres DB
- `ASSET_DB_PASS` Adgangskode til Asset Postgres DB
- `ASSET_DB_HOST` Hostname til Asset Postgres DB
- `ASSET_DB_DATABASE` Databasenavn til Asset Postgres DB
- `ASSET_DB_PORT` Portnummer til Asset Postgres DB


## Cronjob

Cronjobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver mandag  
- **Eksempler på kommende kørsler:**
  - 2025-08-11 00:00:00
  - 2025-08-18 00:00:00
  - 2025-08-25 00:00:00
  - 2025-09-01 00:00:00
  - 2025-09-08 0:00:00
- **Cron syntax:**  
  ```
  0 0 * * 1
  ```