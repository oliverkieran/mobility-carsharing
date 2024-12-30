import datetime
import requests
import json
import os
import pandas as pd
import pytz
from dotenv import load_dotenv

from save import AzureBlobStorage


class Scraper:
    def __init__(self, base_url: str, station_id: str, output_dir: str = "./data/raw"):
        load_dotenv()
        self.base_url = base_url
        self.station_id = station_id
        self.output_dir = output_dir
        self.rental_duration = datetime.timedelta(hours=3)
        azure_blob_connection_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
        if not azure_blob_connection_string:
            raise ValueError(
                "AZURE_BLOB_CONNECTION_STRING environment variable not set"
            )

        self.azure_blob_client = AzureBlobStorage(azure_blob_connection_string)
        if self.azure_blob_client:
            print("Azure Blob Storage client created successfully.")

    def round_to_next_half_hour(self, dt: datetime.datetime) -> datetime.datetime:
        print("Original datetime:", dt)
        total_minutes = dt.hour * 60 + dt.minute

        # Integer division by 30 to find the current half-hour block
        # +1 to move to the *next* half-hour block
        next_half_hour_block = (total_minutes // 30) + 1

        # Convert that block index back into hours and minutes
        new_total_minutes = next_half_hour_block * 30
        new_hour, new_minute = divmod(new_total_minutes, 60)

        # Check if we've rolled over into the next day
        day_offset = new_hour // 24
        new_hour = new_hour % 24

        # Build the new datetime object
        new_dt = dt.replace(hour=new_hour, minute=new_minute, second=0, microsecond=0)
        new_dt += datetime.timedelta(days=day_offset)
        print("New datetime:", new_dt)
        return new_dt

    def _get_filter_data(self, dt: datetime.datetime):
        return {
            "includeWalkingDistance": True,
            "includeWalkingDuration": True,
            "tripmode": "RETURN",
            "paginationCriteria": {"page": 0, "size": 30},
            "coordinatesCriteria": {
                "longitude": 8.437079228460789,
                "latitude": 47.14424096318203,
            },
            "boundariesCriteria": {
                "bottomRight": {
                    "longitude": 8.441263474524021,
                    "latitude": 47.139687270534495,
                },
                "topRight": {
                    "longitude": 8.441263474524021,
                    "latitude": 48.14948014616674,
                },
                "topLeft": {
                    "longitude": 7.432894982397557,
                    "latitude": 48.14948014616674,
                },
                "bottomLeft": {
                    "longitude": 7.432894982397557,
                    "latitude": 47.139687270534495,
                },
            },
            "availabilityCriteria": {
                "fromDateTime": dt.isoformat(timespec="milliseconds"),
                "flexibleFromDateTimeInMinutes": 0,
                "toDateTime": (dt + self.rental_duration).isoformat(
                    timespec="milliseconds"
                ),
                "flexibleToDateTimeInMinutes": 0,
                "categoryIds": [],
                "includeAlternatives": True,
                "includeUnavailables": True,
                "gearShifts": ["AUTOMATIC", "MANUAL"],
                "fuelTypes": ["FOSSIL", "ELECTRIC"],
                "only4x4": False,
                "onlyLearnerDrivers": False,
            },
            "stationCriteria": [self.station_id],
        }

    def fetch_data(self, dt: datetime.datetime):
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.mobility.ch",
            "Referer": "https://www.mobility.ch/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        }

        # Request data
        filter_data = self._get_filter_data(dt)

        availabilities_data = {
            "tx_mobilitymaps_locationmapajaxendpoint[request]": "availabilities",
            "tx_mobilitymaps_locationmapajaxendpoint[filter]": json.dumps(filter_data),
        }

        try:
            response = requests.post(
                self.base_url, headers=headers, data=availabilities_data
            )
            response.raise_for_status()  # Check if request was successful
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def prepare_data_for_file(
        self, data: dict, dt: datetime.datetime, fetch_old_data=True
    ):
        # save vehicle information in pandas dataframe
        df_vehicle_availabilities = pd.json_normalize(data, "vehicleAvailabilities")
        df_vehicle_availabilities["timestamp.from"] = dt.isoformat(
            timespec="milliseconds"
        )
        df_vehicle_availabilities["timestamp.to"] = (
            dt + self.rental_duration
        ).isoformat(timespec="milliseconds")

        if fetch_old_data:
            # Load previous data from azure blob storage
            df_previous_data = self.azure_blob_client.download_blob(
                "data", "availabilities_data.csv"
            )
            # Concatenate previous data with new data
            df_vehicle_availabilities = pd.concat(
                [df_previous_data, df_vehicle_availabilities]
            )

        return df_vehicle_availabilities

    def run(self, fetch_old_data=True):
        dt = datetime.datetime.now(pytz.timezone("Europe/Zurich")) + datetime.timedelta(
            hours=0
        )
        rounded_dt = self.round_to_next_half_hour(dt)
        result = self.fetch_data(rounded_dt)
        if result:
            print(result)
            df_prepared_data = self.prepare_data_for_file(
                result, rounded_dt, fetch_old_data
            )
            # self.azure_blob_client.upload_blob(
            #     df_prepared_data, "data", "availabilities_data.csv"
            # )
            return df_prepared_data


if __name__ == "__main__":
    interesting_station_ids = [69951, 80456, 80815]
    # with open("availabilities/69951_2024-12-26.json", "r") as file:
    #     data = json.load(file)
    #     prepare_data_for_file(data)
    scraper = Scraper("https://www.mobility.ch/en/api/locationmap", 69951)
    scraper.run()
