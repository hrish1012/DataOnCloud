# from src.ingestion.connectors.file_connector import FileConnector
# from src.ingestion.connectors.api_connector import APIConnector

# # File connector test
# file_connector = FileConnector(file_path="data/raw/customers.csv", source_name="customers_csv")
# file_result = file_connector.run()
# print("\n--- File Connector Result ---")
# print(file_result)

# # API connector test
# api_connector = APIConnector(url="https://jsonplaceholder.typicode.com/users", source_name="users_api")
# api_result = api_connector.run()
# print("\n--- API Connector Result ---")
# print(api_result)

from src.ingestion.connector_factory import load_connectors

connectors = load_connectors()

for connector in connectors:
    result = connector.run()
    print(f"\n--- {result.source_name} ---")
    print(result)