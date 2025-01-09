import json
import csv
import sys
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential

#===============================
# How to use this script:
# Provide your client_id, tenant_id, client_secret in the following lines , then fill the columnsSensitivitymap.csv
# with your Desired classification and its Sensitivity Label
# Please use following formating :
#ClassificationType,ClassificationName
#Secret,ClassifierName
#High_Confiential,ClassifierName
#Confidential,Classifier_Name
#Internal_Usage,Classifier_Name
#Public,Classifier_Name
# Once the mapping file has been filed, run the script.

# Azure service principal credentials (replace with your own values)
client_id = ""
tenant_id = ""
client_secret = ""


# Define file paths
json_file_path = 'combined_entity_details.json'
sensitivity_map_file_path = 'columnsSensitivitymap.csv'
csv_file_path = 'entity_classifications.csv'
output_blob_name = "entity_classifications.csv"



# Authenticate using service principal

# Load sensitivity classifications from CSV
sensitivity_map = {}
try:
    with open(sensitivity_map_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            classification_type = row['ClassificationType']
            classification_name = row['ClassificationName']
            if classification_type not in sensitivity_map:
                sensitivity_map[classification_type] = set()
            sensitivity_map[classification_type].add(classification_name)

    highly_confidential_classifications = sensitivity_map.get('High_Confidential', set())
    confidential_classifications = sensitivity_map.get('Confidential', set())
    secret_classifications = sensitivity_map.get('Secret', set())
    internal_usage_classification = sensitivity_map.get('Internal_Usage', set())
    public_usage_classification = sensitivity_map.get('Public', set())

except Exception as e:
    print(f"Failed to load sensitivity classifications: {e}")
    exit(1)

# Load the JSON data
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Extract entities and their classifications in detail
entity_classifications_detail = []

for entry in data:
    entities = entry.get('entities', [])
    referred_entities = entry.get('referredEntities', {})

    for entity in entities:
        entity_info = {
            'Entity Name': entity.get('attributes', {}).get('name', ''),
            'Entity GUID': entity.get('guid', ''),
            'Entity Type': entity.get('typeName', ''),
            'Entity Classifications': [],
            'Columns': []
        }

        # Check for columns and their classifications
        relationship_attributes = entity.get('relationshipAttributes', {})
        columns = relationship_attributes.get('columns', [])
        for column in columns:
            column_guid = column.get('guid', '')
            referred_entity = referred_entities.get(column_guid, {})
            column_info = {
                'Column Name': column.get('displayText', ''),
                'Column GUID': column_guid,
                'Data Type': referred_entity.get('attributes', {}).get('data_type', ''),
                'Size': referred_entity.get('attributes', {}).get('length', ''),
                'Column Classifications': []
            }
            # Check for classifications in referredEntities
            classifications = referred_entity.get('classifications', [])
            if classifications:
                column_info['Column Classifications'] = [cls.get('typeName') for cls in classifications]

            # Determine data sensitivity
            classification_set = set(column_info['Column Classifications'])
            if classification_set & highly_confidential_classifications:
                column_info['Data Sensitivity'] = 'High Confidential'
            elif classification_set & confidential_classifications:
                column_info['Data Sensitivity'] = 'Confidential'
            elif classification_set & secret_classifications:
                column_info['Data Sensitivity'] = 'Secret'
            elif classification_set & internal_usage_classification:
                column_info['Data Sensitivity'] = 'Internal Usage'
            elif classification_set & public_usage_classification:
                column_info['Data Sensitivity'] = 'Public'
            else:
                column_info['Data Sensitivity'] = 'Not Classified'

            entity_info['Columns'].append(column_info)

        # Check for entity classifications
        entity_classifications = entity.get('classifications', [])
        if entity_classifications:
            entity_info['Entity Classifications'] = [cls.get('typeName') for cls in entity_classifications]

        entity_classifications_detail.append(entity_info)

# Flatten the data for CSV
flat_data = []
for entity in entity_classifications_detail:
    for column in entity['Columns']:
        flat_data.append({
            'Entity Name': entity['Entity Name'],
            'Entity GUID': entity['Entity GUID'],
            'Column Name': column['Column Name'],
            'Column GUID': column['Column GUID'],
            'Data Type': column['Data Type'],
            'Size': column['Size'],
            'Entity Classifications': ', '.join(entity['Entity Classifications']),
            'Column Classifications': ', '.join(column['Column Classifications']),
            'Data Sensitivity': column['Data Sensitivity']
        })

# Write to CSV
with open(csv_file_path, 'w', newline='') as csvfile:
    fieldnames = [
        'Entity Name', 'Entity GUID', 'Column Name', 'Column GUID',
        'Data Type', 'Size', 'Entity Classifications', 'Column Classifications', 'Data Sensitivity'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in flat_data:
        writer.writerow(row)

print(f'Data has been written to {csv_file_path}')