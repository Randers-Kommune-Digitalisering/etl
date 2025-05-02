from utils.config import BOM_USERNAME, BOM_PASSWORD, BROWSERLESS_URL
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_bom_data():
    login_url = 'https://sag.bygogmiljoe.dk/'
    url = f"{BROWSERLESS_URL.rstrip('/')}/function"
    headers = {
        "Content-Type": "application/javascript",
    }
    data = """
        module.exports = async ({page}) => {
            try {
                console.log("Start BOM RPA job");
                await page.goto('""" + f"{login_url}" + """', { waitUntil: 'networkidle2' });
                console.log("Navigating to login page...");

                // Step 1: Select kommune
                try {
                    console.log("Locating kommune dropdown...");
                    await page.waitForSelector('form div div div select');
                    const kommuneDropdown = await page.$('form div div div select');
                    await kommuneDropdown.click();
                    await page.keyboard.type('Randers Kommune (RPA)');
                    await page.keyboard.press('Enter');
                    console.log("Randers Kommune (RPA) selected.");
                } catch (error) {
                    console.error("Failed to select kommune:", error);
                    return { success: false, error: "Failed to select kommune" };
                }

                // Step 2: Click Fortsæt button
                try {
                    console.log("Clicking on the Fortsæt button...");
                    await page.waitForSelector('form div div a');
                    const fortsætButton = await page.$('form div div a');
                    await fortsætButton.click();
                    await page.waitForTimeout(2000);
                    console.log("Fortsæt button clicked.");
                } catch (error) {
                    console.error("Failed to click Fortsæt button:", error);
                    return { success: false, error: "Failed to click Fortsæt button" };
                }

                // Step 3: Enter username and password
                try {
                    console.log("Entering username...");
                    await page.waitForSelector('#userNameInput');
                    const usernameInput = await page.$('#userNameInput');
                    await usernameInput.click();
                    await usernameInput.type('""" + BOM_USERNAME + """');
                    console.log("Username entered.");

                    console.log("Entering password...");
                    await page.waitForSelector('#passwordInput');
                    const passwordInput = await page.$('#passwordInput');
                    await passwordInput.click();
                    await passwordInput.type('""" + BOM_PASSWORD + """');
                    console.log("Password entered.");
                } catch (error) {
                    console.error("Failed to enter username or password:", error);
                    return { success: false, error: "Failed to enter username or password" };
                }

                // Step 4: Click Submit button
                try {
                    console.log("Clicking on the Submit button...");
                    await page.waitForSelector('#submitButton');
                    const submitButton = await page.$('#submitButton');
                    await submitButton.click();
                    await page.waitForTimeout(5000);
                    console.log("Submit button clicked.");
                } catch (error) {
                    console.error("Failed to click Submit button:", error);
                    return { success: false, error: "Failed to click Submit button" };
                }

                // Step 5: Navigate to Statistik og Servicemål
                try {
                    console.log("Clicking on the Menu nav element...");
                    await page.waitForXPath('/html/body/div/header/div/div/div[2]/nav');
                    const navElement = await page.$x('/html/body/div/header/div/div/div[2]/nav');
                    await navElement[0].click();
                    console.log("Nav element clicked.");

                    console.log("Clicking on the Statistik og Servicemal element...");
                    await page.waitForSelector('body > div > header > div > div > div.span4.offset2 > nav > ul > li > ul > li:nth-child(4) > a');
                    const statistikOgServicemaalElement = await page.$('body > div > header > div > div > div.span4.offset2 > nav > ul > li > ul > li:nth-child(4) > a');
                    await statistikOgServicemaalElement.click();
                    console.log("Statistik og Servicemal element clicked.");
                } catch (error) {
                    console.error("Failed to navigate to Statistik og Servicemal:", error);
                    return { success: false, error: "Failed to navigate to Statistik og Servicemal" };
                }

                // Step 6: Handle Sagsomrode
                try {
                    console.log("Clicking on the Sagomrode button...");
                    await page.waitForSelector('form > div:nth-child(2) > div > div:nth-child(2) > button');
                    const sagomraadeButton = await page.$('form > div:nth-child(2) > div > div:nth-child(2) > button');
                    await sagomraadeButton.click();
                    console.log("Sagomrode button clicked.");

                    console.log("Clicking on the Byg field...");
                    await page.waitForSelector('form > div:nth-child(2) > div > div:nth-child(2) > ul > li:nth-child(2) > a > label > input');
                    const bygField = await page.$('form > div:nth-child(2) > div > div:nth-child(2) > ul > li:nth-child(2) > a > label > input');
                    await bygField.click();
                    console.log("Byg field clicked.");
                } catch (error) {
                    console.error("Failed to handle Sagomrode:", error);
                    return { success: false, error: "Failed to handle Sagomrode" };
                }

                // Step 7: Handle Servicemal
                try {
                    console.log("Clicking on the Servicemal button...");
                    await page.waitForSelector('form > div:nth-child(2) > div > div:nth-child(4) > button');
                    const servicemaalButton = await page.$('form > div:nth-child(2) > div > div:nth-child(4) > button');
                    await servicemaalButton.click();
                    console.log("Servicemal button clicked.");

                    console.log("Clicking on the checkboxes...");
                    const checkboxes = [
                        'body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(2) > a > label',
                        'body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(3) > a > label',
                        'body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(4) > a > label',
                        'body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(5) > a > label',
                        'body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(6) > a > label'
                    ];
                    for (const selector of checkboxes) {
                        await page.waitForSelector(selector, { visible: true });

                        // Extract and log the text of the checkbox
                        const checkboxText = await page.evaluate((selector) => {
                            const labelElement = document.querySelector(selector);
                            return labelElement ? labelElement.textContent.trim() : null;
                        }, selector);

                        if (checkboxText) {
                            console.log(`Checkbox text: ${checkboxText}`);
                        } else {
                            console.error(`Could not extract text for checkbox: ${selector}`);
                        }

                        const checkbox = await page.$(selector);
                        if (checkbox) {
                            await checkbox.click();
                            console.log(`Checkbox ${selector} clicked.`);
                        } else {
                            console.error(`Checkbox ${selector} not found.`);
                        }
                    }
                } catch (error) {
                    console.error("Failed to handle Servicemal:", error);
                    return { success: false, error: "Failed to handle Servicemal" };
                }

                // Step 8: Handle date fields for multiple months
                try {
                    console.log("Starting to iterate over months...");

                    console.log("Clicking on the button to make the date fields visible...");
                    await page.waitForSelector('#datepicker > input:nth-child(2)', { visible: true });
                    const dateButton = await page.$('#datepicker > input:nth-child(2)');
                    await dateButton.click();
                    console.log("Date button clicked.");

                    const today = new Date();
                    let currentMonth = new Date(2023, 8, 1);
                    let isFirstIteration = true;

                    const allData = [];

                    while (currentMonth.getMonth() < 12 && currentMonth.getFullYear() === 2023) {
                        const firstDayOfMonth = new Date(currentMonth.getFullYear(), currentMonth.getMonth(), 1);
                        const lastDayOfMonth = new Date(currentMonth.getFullYear(), currentMonth.getMonth() + 1, 0);

                        const options = { day: '2-digit', month: '2-digit', year: 'numeric' };
                        const fraDato = firstDayOfMonth.toLocaleDateString('da-DK', options).replace(/\./g, '-');
                        const tilDato = lastDayOfMonth.toLocaleDateString('da-DK', options).replace(/\./g, '-');

                        console.log(`Setting date range: Fra Dato = ${fraDato}, Til Dato = ${tilDato}`);

                        await page.waitForSelector('#datepicker > input:nth-child(1)', { visible: true });
                        const fraDatoInput = await page.$('#datepicker > input:nth-child(1)');
                        await fraDatoInput.click({ clickCount: 3 });
                        await fraDatoInput.type(fraDato);
                        console.log(`Fra Dato set to: ${fraDato}`);

                        await page.waitForSelector('#datepicker > input:nth-child(2)', { visible: true });
                        const tilDatoInput = await page.$('#datepicker > input:nth-child(2)');
                        await tilDatoInput.click({ clickCount: 3 });
                        await tilDatoInput.type(tilDato);
                        console.log(`Til Dato set to: ${tilDato}`);

                        if (isFirstIteration) {
                            // Step 9: Click on the Nogletal button (only for the first iteration)
                            try {
                                console.log("Clicking on the Nogletal button...");
                                await page.waitForSelector('#servicemaal-result-toggler > button:nth-child(2)', { visible: true });
                                const noegtalButton = await page.$('#servicemaal-result-toggler > button:nth-child(2)');
                                await noegtalButton.click();
                                await page.waitForTimeout(5000);
                                console.log("Nogletal button clicked.");
                            } catch (error) {
                                console.error("Failed to click Nogletal button:", error);
                                return { success: false, error: "Failed to click Nogletal button" };
                            }
                            isFirstIteration = false;
                        } else {
                            // Step 9: Click on the Sog button (for sub iterations)
                            try {
                                console.log("Clicking on the Sog button...");
                                await page.waitForSelector('body > div > div.container > form > div:nth-child(3) > div > span > button.btn.btn-primary', { visible: true });
                                const SogButton = await page.$('body > div > div.container > form > div:nth-child(3) > div > span > button.btn.btn-primary');
                                await SogButton.click();
                                await page.waitForTimeout(5000);
                                console.log("Sog button clicked.");
                            } catch (error) {
                                console.error("Failed to click Sog button:", error);
                                return { success: false, error: "Failed to click Sog button" };
                            }
                        }

                        // Step 10: Extract data
                        try {
                            console.log("Extracting data...");
                            const fraDato = await page.$eval('#datepicker > input:nth-child(1)', el => el.value);
                            const tilDato = await page.$eval('#datepicker > input:nth-child(2)', el => el.value);

                            const servicemaalGennemsnit = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(1) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingGennemsnit = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(1) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentGennemsnit = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(1) > td:nth-child(14)', el => el.textContent.trim());

                            const servicemaalSimple = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(2) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingSimple = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(2) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentSimple = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(2) > td:nth-child(14)', el => el.textContent.trim());

                            const servicemaalEnfamilieshuse = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(3) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingEnfamilieshuse = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(3) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentEnfamilieshuse = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(3) > td:nth-child(14)', el => el.textContent.trim());

                            const servicemaalIndustri = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(4) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingIndustri = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(4) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentIndustri = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(4) > td:nth-child(14)', el => el.textContent.trim());

                            const servicemaalEtageErhverv = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(5) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingEtageErhverv = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(5) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentEtageErhverv = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(5) > td:nth-child(14)', el => el.textContent.trim());

                            const servicemaalEtageBoliger = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(6) > td:nth-child(1)', el => el.textContent.trim());
                            const sagsbehandlingEtageBoliger = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(6) > td:nth-child(4)', el => el.textContent.trim());
                            const servicemaalProcentEtageBoliger = await page.$eval('#servicemaal-noegletal-table > tbody > tr:nth-child(6) > td:nth-child(14)', el => el.textContent.trim());

                            console.log("Data extracted successfully:");
                            console.log(`Fra Dato: ${fraDato}, Til Dato: ${tilDato}`);
                            console.log(`Servicemal Gennemsnit: ${servicemaalGennemsnit}, Sagsbehandling Gennemsnit: ${sagsbehandlingGennemsnit}, Servicemal Procent Gennemsnit: ${servicemaalProcentGennemsnit}`);
                            console.log(`Servicemal Simple: ${servicemaalSimple}, Sagsbehandling Simple: ${sagsbehandlingSimple}, Servicemal Procent Simple: ${servicemaalProcentSimple}`);
                            console.log(`Servicemal Enfamilieshuse: ${servicemaalEnfamilieshuse}, Sagsbehandling Enfamilieshuse: ${sagsbehandlingEnfamilieshuse}, Servicemal Procent Enfamilieshuse: ${servicemaalProcentEnfamilieshuse}`);
                            console.log(`Servicemal Industri: ${servicemaalIndustri}, Sagsbehandling Industri: ${sagsbehandlingIndustri}, Servicemal Procent Industri: ${servicemaalProcentIndustri}`);
                            console.log(`Servicemal Etage Erhverv: ${servicemaalEtageErhverv}, Sagsbehandling Etage Erhverv: ${sagsbehandlingEtageErhverv}, Servicemal Procent Etage Erhverv: ${servicemaalProcentEtageErhverv}`);
                            console.log(`Servicemal Etage Boliger: ${servicemaalEtageBoliger}, Sagsbehandling Etage Boliger: ${sagsbehandlingEtageBoliger}, Servicemal Procent Etage Boliger: ${servicemaalProcentEtageBoliger}`);
                            console.log("Data extraction completed for this month.");

                            allData.push({
                                "Fra Dato": fraDato,
                                "Til Dato": tilDato,
                                "Kategori": [
                                    servicemaalGennemsnit, servicemaalSimple, servicemaalEnfamilieshuse,
                                    servicemaalIndustri, servicemaalEtageErhverv, servicemaalEtageBoliger
                                ],
                                "Sagsbehandling": [
                                    sagsbehandlingGennemsnit, sagsbehandlingSimple, sagsbehandlingEnfamilieshuse,
                                    sagsbehandlingIndustri, sagsbehandlingEtageErhverv, sagsbehandlingEtageBoliger
                                ],
                                "Servicemal Procent": [
                                    servicemaalProcentGennemsnit, servicemaalProcentSimple, servicemaalProcentEnfamilieshuse,
                                    servicemaalProcentIndustri, servicemaalProcentEtageErhverv, servicemaalProcentEtageBoliger
                                ]
                            });

                            console.log("Data extracted successfully for this month.");

                        } catch (error) {
                            console.error("Failed to extract data:", error);
                            return { success: false, error: "Failed to extract data" };
                        }

                        console.log("Moving to the next month...")
                        currentMonth.setMonth(currentMonth.getMonth() + 1);
                    }

                    console.log("Finished iterating over all months.");

                    return {
                        data: {
                            allData
                        },
                        type: 'application/json'
                    };

                } catch (error) {
                    console.error("Failed to handle date fields:", error);
                    return { success: false, error: "Failed to handle date fields" };
                }
            } catch (error) {
                console.error("An unexpected error occurred:", error);
                return { success: false, error: error.message };
            }
        }
        """
    return url, headers, data


def process_and_save_bom_data(response_json):
    try:
        all_data = response_json.get("allData", [])

        if not all_data:
            logger.info("No data found in response.")
            return None

        all_rows = []

        for date_data in all_data:
            logger.info(f"Processing Date data: {date_data}")

            fra_dato = date_data.get("Fra Dato", "").strip()
            til_dato = date_data.get("Til Dato", "").strip()
            kategori = date_data.get("Kategori", [])
            sagsbehandling = date_data.get("Sagsbehandling", [])
            servicemaal_procent = date_data.get("Servicemal Procent", [])

            logger.info(f"Fra Dato: {fra_dato}, Til Dato: {til_dato}")
            logger.info(f"Kategori: {kategori}")
            logger.info(f"Sagsbehandling: {sagsbehandling}")
            logger.info(f"Servicemål i procent: {servicemaal_procent}")

            for i in range(len(kategori)):
                all_rows.append({
                    "Fra Dato": fra_dato,
                    "Kategori": kategori[i] if i < len(kategori) else None,
                    "Sagsbehandlingstid": sagsbehandling[i] if i < len(sagsbehandling) else None,
                    "Servicemål i procent": servicemaal_procent[i] if i < len(servicemaal_procent) else None
                })

        df = pd.DataFrame(all_rows)

        logger.info("Data processed successfully.")
        return df

    except Exception as e:
        logger.error(f"Failed to process and save data: {e}")
        return None
