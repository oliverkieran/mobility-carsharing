from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
from io import BytesIO
from io import StringIO


class AzureBlobStorage:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

    def create_container(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()
            print(f"Container {container_name} created.")
        else:
            print(f"Container {container_name} already exists.")

    def upload_blob(self, data: pd.DataFrame, container_name: str, blob_name: str):
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        csv_data = StringIO()
        data.to_csv(csv_data, index=False)
        csv_data.seek(0)
        blob_client.upload_blob(csv_data.getvalue(), overwrite=True)
        print(f"Blob {blob_name} uploaded to container {container_name}.")

    def download_blob(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        blob_data = blob_client.download_blob().readall()
        if blob_name.endswith(".csv"):
            return pd.read_csv(BytesIO(blob_data))
        return blob_data


def save_data_locally(df_data, output_dir="./data/raw"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not os.path.exists(f"{output_dir}/availabilities_data.csv"):
        df_data.to_csv(f"{output_dir}/availabilities_data.csv", index=False)
    else:
        df_data.to_csv(
            f"{output_dir}/availabilities_data.csv",
            mode="a",
            index=False,
            header=False,
        )
