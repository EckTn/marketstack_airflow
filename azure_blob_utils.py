from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

#Load json config file
config_file_path = 'config.json'
with open (config_file_path, 'r') as file:
    config = json.load(file)

connection_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_name = config['container_name']
local_csv_path = config['local_csv_path']
blob_name = os.path.basename(local_csv_path)

blob_service_client = BlobServiceClient.from_connection_string(connection_str)
container_client = blob_service_client.get_container_client(container_name)

with open(local_csv_path, 'rb') as data:
    container_client.upload_blob(name=blob_name, data=data, overwrite=True)

print(f"Uploded {local_csv_path} to Azure Blob {blob_name} in container {container_name}")
