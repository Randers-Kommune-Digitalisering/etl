# Zylinc ETL Job `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Cronjob**](#cronjob)

## Formål

Formålet med jobbet er at hente telefoni data fra flere Zylinc-køer i Elasticsearch og gemme dem i en PostgreSQL-database. Hver kø får sin egen tabel i Postgres databasen.

## Beskrivelse

Kode består af et ETL-job, der udfører følgende trin:

- Connecter til Elasticsearch og henter telefoni data for hver kø i listen
(`get_queue_names()`)
- Følgende data hentes i Elasticsearch: (`QueueName`, `Result`, `AgentDisplayName`, `ConversationEventType`, `StartTimeUtc`, `TotalDurationInMilliseconds`, `EventDurationInMilliseconds`)
- Dataen gemmes i en Postgres Database, én tabel pr. kø (`zylinc_<kønavn>`)

**Dataflow:**
- Data fra Elasticsearch → Hver kø gemmes i deres egen tabel i Postgres DB

## Afhængigheder

Installér afhængigheder med:

```bash
pip install -r src/requirements.txt
```

:key: | **Miljøvariabler**

- `ZYLINC_POSTGRES_DB_USER` Brugernavn til Zylinc Postgres DB
- `ZYLINC_POSTGRES_DB_PASS` Adgangskode til Zylinc Postgres DB
- `ZYLINC_POSTGRES_DB_HOST` Hostname til Zylinc Postgres DB
- `ZYLINC_POSTGRES_DB_DATABASE` Databasenavn til Zylinc Postgres DB 
- `ZYLINC_POSTGRES_DB_PORT` Portnummer til Zylinc Postgres DB


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