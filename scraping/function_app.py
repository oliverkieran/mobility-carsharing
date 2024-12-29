import logging
import azure.functions as func
from datetime import datetime
from scraper import Scraper

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 */3 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False
)
def ScraperFunction(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=None)
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)

    scraper = Scraper("https://www.mobility.ch/en/api/locationmap", 80815)
    try:
        scraper.run()
        logging.info("Scraper executed successfully.")
    except Exception as e:
        logging.error(f"Error during scraping: {str(e)}")
