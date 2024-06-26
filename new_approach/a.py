from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient

account_url = "https://storage1.blob.core.windows.net/"

creds = DefaultAzureCredential()
service_client = BlobServiceClient(
    account_url=account_url,
    credential=creds
)
