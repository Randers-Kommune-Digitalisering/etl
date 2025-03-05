# Overblik over ydelsesgrupper fra Jobindsats.dk med datasættet Y30R21

# Formål med Projektet

Dette projekt har til formål at udvikle en ETL (Extract, Transform, Load) service for Jobinsats API'et med fokus på Datasættet [Y30R21] som omhandler de forskellige ydelsesgrupper

ETL-processen indebærer følgende trin:

1. Extract: Data hentes fra alle kommuner i Danmark via Jobinsats API'et.

2. Transform: De extracted data behandles og organiseres med fokus på specifikke ydelsesgrupper, områder(Kommuner) og perioder.

3. Load: De transformerede data indlæses i SAP BusinessObjects (SAP BO) til videre analyse, rapportering og visualiseringer.

## Ydelsesgrupper

Projektet fokuserer på følgende ydelsesgrupper:
* A-Dagpenge: Data relateret til arbejdsløshedsdagpenge.
* Kontanthjælp: Data relateret til kontanthjælp.
* Sygedagpenge: Data relateret til sygedagpenge.
* Ydelsesgrupper i alt: Samlede data for alle ydelsesgrupper.

## Eksempler fra Jobindsats API'et
[Brug af API'et](https://jobindsats.dk/media/fzvhjlqu/api-eksempler.pdf)

[Brugerdokumentation](https://jobindsats.dk/media/myrlvikp/brugerdokumentation.pdf)

## Strukturen i projektet
ETL scripts for Jobindsats API'et placeres under mappen ``` src/jobindsats```

## Kørsel af jobindsat.py Servicen
* ENV variable for JOBINDSATS_API_KEY(Bitwarden)
* Start applikationen: ```python src/main.py```
* Start jobindsats Servicen: Trigger jobindsat servicen med følgende POST request i Postman ```http://127.0.0.1:8080/jobs/jobindsats```
* Derefter bliver alle ETL scripts under ``` src/jobindsats``` blive kørt


## Opstart af Custom Data Connector(CDC) Lokalt på sin maskine
* ENV variable for CUSTOM_DATA_CONNECTOR_HOST
* Start container: ```Path: dev-tools```  ```docker-compose up ```
* Eksempel på at hente en specifik fil: ```http://127.0.0.1:1880/in/Meta_test.csv ```