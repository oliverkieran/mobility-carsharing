import logging
import azure.functions as func
from datetime import datetime
from scraper import Scraper
import pandas as pd

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 */3 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False
)
def ScraperFunction(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=None)
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
    interesting_station_ids = [69951, 80456, 80815, 70151]
    df_new_data = None
    for i, station_id in enumerate(interesting_station_ids):
        scraper = Scraper("https://www.mobility.ch/en/api/locationmap", station_id)
        try:
            df_scraped_data = scraper.run(fetch_old_data=True if i == 0 else False)
            if i == 0:
                df_new_data = df_scraped_data
            else:
                df_new_data = pd.concat([df_new_data, df_scraped_data])
            logging.info(f"Scraper executed successfully for station {station_id}.")
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
    scraper.azure_blob_client.upload_blob(
        df_new_data, "data", "availabilities_data.csv"
    )
