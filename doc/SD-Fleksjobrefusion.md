# SD-Fleksjobrefusion ETL Job `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Fremgangsmåde**](#fremgangsmåde) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Cronjob**](#cronjob)

## Formål

Formålet med applikationen er at automatisere indberetning af fleksjobrefusion i SD Personaleweb ved at læse løndata fra et Excel-ark på SFTP, logge ind i SD via Selenium, og indberette relevante beløb for hver medarbejder.

## Beskrivelse

Applikationen består af et ETL-script, der udfører følgende trin:

- Logger ind på SD Personaleweb via Selenium WebDriver (Chrome i headless mode).
- Henter det nyeste Excel-ark med løndata fra en SFTP-server.
- Læser og parser Excel-arket for relevante felter (tjenestenummer, institution, beløb, lønart).
- For hver medarbejder i arket indberettes beløbet automatisk i SD-systemet via webinterfacet.
- Logger eventuelle fejl og rapporterer hvilke medarbejdere, der evt. ikke kunne behandles.

**Dataflow:**
- SFTP → Excel → Selenium (SD Personaleweb) → Indberetning

## Fremgangsmåde

1. En medarbejder fra Personale og HR uploader manuelt den nyeste Excel-fil med løndata til den aftalte SFTP-mappe.
2. Jobbet henter automatisk den nyeste fil fra SFTP, behandler data og indberetter i SD Personaleweb.
3. Efter kørsel kan loggen gennemgås for eventuelle fejl eller manglende indberetninger ved hver person

## Kørsel lokalt (uden headless / med browser-vindue)

Hvis du ønsker at køre jobbet lokalt og følge processen i browseren (ikke headless):

1. **Fjern eller udkommenter linjen med `--headless` i din `Options`-opsætning i `fleksjobrefusion.py`:**

    ```python
    options = Options()
    options.add_argument("--incognito")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    ```

2. **Hvis du kører scriptet lokalt på din PC på Randers Kommunes netværk/ADFS, kan du udkommentere følgende linjer i `fleksjobrefusion_data.py` for at automatisere login med pyautogui:**

    ```python
    time.sleep(2)
    pyautogui.write(SD_FLEKSJOBREFUSION_USERNAME)
    pyautogui.press('tab')
    pyautogui.write(SD_FLEKSJOBREFUSION_PASSWORD)
    pyautogui.press('enter')
    ```

## Afhængigheder

Installér afhængigheder med:

```bash
pip install -r src/requirements.txt
```

:key: | **Miljøvariabler**

- `SD_FLEKSJOBREFUSION_USERNAME` Brugernavn til SD Personaleweb
- `SD_FLEKSJOBREFUSION_PASSWORD` Adgangskode til SD Personaleweb
- `SD_FLEKSJOBREFUSION_SFTP_USER` Brugernavn til SFTP
- `SD_FLEKSJOBREFUSION_SFTP_PASS` Password til SFTP
- `SD_FLEKSJOBREFUSION_SFTP_HOST` Host til SFTP
- `SD_FLEKSJOBREFUSION_URL` URL til SD Personaleweb
- `SD_FLEKSJOBREFUSION_SFTP_DIR` SFTP-sti til Excel


## Cronjob

Cronjobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 09:00 den første dag i hver måned  
- **Eksempler på kommende kørsler:**
  - 2025-07-01 09:00:00
  - 2025-08-01 09:00:00
  - 2025-09-01 09:00:00
  - 2025-10-01 09:00:00
  - 2025-11-01 09:00:00
- **Cron syntax:**  
  ```
  0 9 1 * *
  ```