import json
import os
import sys
import pandas as pd
from azure.storage.blob import BlobServiceClient
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient
import chardet as ch
from azure.identity import ClientSecretCredential

# Azure App Registration credentials
client_id = ""
tenant_id = ""
client_secret = ""
purview_account_name = ""

if __name__ == "__main__":
    # Authenticate using Service Principal for Azure Purview
    oauth = ServicePrincipalAuthentication(
        client_id=client_id,
        tenant_id=tenant_id,
        client_secret=client_secret
    )

    client = PurviewClient(
        account_name=purview_account_name,
        authentication=oauth
    )

    # Authenticate using Service Principal for Azure Blob Storage
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    local_csv_file = "entity_details.csv"

    # Download CSV file from Blob Storage


    # Detect the encoding of the CSV file
    try:
        with open(local_csv_file, 'rb') as file:
            result = ch.detect(file.read())  # Detect the file encoding
            detected_encoding = result['encoding']
        print(f"Detected encoding: {detected_encoding}")

        # Read the CSV file with the detected encoding
        df = pd.read_csv(local_csv_file, encoding=detected_encoding)
        guids = df['GUID'].tolist()
    except Exception as e:
        print(f"Failed to read CSV file {local_csv_file}: {e}")
        exit(1)

    all_entity_details = []

    for guid in guids:
        try:
            entity_details = client.get_entity(guid)
            all_entity_details.append(entity_details)
            print(f"Details for GUID {guid} fetched.")
        except Exception as e:
            print(f"Failed to fetch details for GUID {guid}: {e}")

    combined_json_file_name = "combined_entity_details.json"

    try:
        with open(combined_json_file_name, "w") as json_file:
            json.dump(all_entity_details, json_file, indent=4, sort_keys=True)
        print(f"All entity details written to {combined_json_file_name}")
    except Exception as e:
        print(f"Failed to write JSON file {combined_json_file_name}: {e}")
        exit(1)
