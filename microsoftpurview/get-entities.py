import requests
import csv
import sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential

# Azure App Registration credentials
client_id = "" # Insert your client ID Here
tenant_id = "" # Insert your Tenant ID Here
purview_account_name = "" # Insert you Purview Account Name Here
client_secret = '' # Insert your client_secret Here

scope = "https://purview.azure.net/.default"  # Scope for Azure Purview API
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Azure Storage Account credentials

# Function to get the access token
def get_access_token():
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
        "grant_type": "client_credentials"
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"Failed to obtain access token: {response.status_code} - {response.text}")
        return None

# Define API endpoints
search_api_url = "https://"+purview_account_name+".purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
entity_details_api_url = "https://"+purview_account_name+".purview.azure.com/datamap/api/atlas/v2/entity/bulk"

# Define the search query payload
search_payload = {
    "keywords": "*",
    "limit": 100  # Adjust the limit as needed
}

# Function to search for entities
def search_entities(access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(search_api_url, json=search_payload, headers=headers)
    if response.status_code == 200:
        print("Search Response:", response.json())  # Debugging line
        return response.json().get('value', [])
    else:
        print(f"Failed to search entities: {response.status_code} - {response.text}")
        return []

# Function to get entity details by GUID
def get_entity_details(guid, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{entity_details_api_url}?guid={guid}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Entity Details Response:", response.json())  # Debugging line
        return response.json().get('entities', [])
    else:
        print(f"Failed to get entity details for GUID {guid}: {response.status_code} - {response.text}")
        return []

# Main script
def main():
    access_token = get_access_token()
    if not access_token:
        print("No access token obtained. Exiting.")
        return

    entities = search_entities(access_token)
    entity_details = []

    for entity in entities:
        guid = entity.get('id')
        if guid:
            details = get_entity_details(guid, access_token)
            if details:
                entity_details.extend(details)

    # Write details to CSV
    csv_file_path = 'entity_details.csv'
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['GUID', 'Name', 'Type', 'Attributes'])

        # Write entity details
        for detail in entity_details:
            guid = detail.get('guid')
            name = detail.get('attributes', {}).get('qualifiedName')
            entity_type = detail.get('typeName')
            attributes = detail.get('attributes')
            writer.writerow([guid, name, entity_type, attributes])

    print("Entity details have been written to entity_details.csv")
if __name__ == "__main__":
    main()
