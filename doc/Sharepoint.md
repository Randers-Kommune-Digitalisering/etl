# Sharepoint ETL Job `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Cronjob**](#cronjob)

## Formål

Formålet med jobbet er at hente data fra en Sharepoint Liste og gemme det i en Postgres DB. Data bliver hentet fra API-Service som har et endpoint udstillet til at hente data fra en speicfik sharepoint liste. 

## Beskrivelse

Kode  består af et ETL-job, der udfører følgende trin:

- Henter Sharepoint liste data fra API-Service som udstillet et endpoint til at hente sharepoint data gennem Microsoft Graph API
- Gemmer data i en Postgres DB

**Dataflow:**
- Data fra API-Service → Gem i Postgres DB 


## Afhængigheder

Installér afhængigheder med:

```bash
pip install -r src/requirements.txt
```

:key: | **Miljøvariabler**

- `SHAREPOINT_POSTGRES_DB_USER` Brugernavn til Sharepoint Postgres DB
- `SHAREPOINT_POSTGRES_DB_PASS` Adgangskode til Sharepoint Postgres DB
- `SHAREPOINT_POSTGRES_DB_HOST` Hostname til Sharepoint Postgres DB
- `SHAREPOINT_POSTGRES_DB_DATABASE` Databasenavn til Sharepoint Postgres DB 
- `SHAREPOINT_POSTGRES_DB_PORT` Portnummer til Sharepoint Postgres DB
- `API_SERVICE_URL` URL til API-Service


## Cronjob

Cronjobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver eneste døgn  
- **Eksempler på kommende kørsler:**
  - 2025-08-07 00:00:00
  - 2025-08-08 00:00:00
  - 2025-08-09 00:00:00
  - 2025-08-10 00:00:00
  - 2025-08-11 0:00:00
- **Cron syntax:**  
  ```
  0 0 * * *
  ```