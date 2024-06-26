from azure.storage.blob import BlobServiceClient

storage_account_key = "eyGvrinmFZ9/GsfhQxQkZRftMdi3A/2Xvp7PEK+B8NgiIXessYMTU6R4RU52F6hur8SHV1+TVDHr+ASt5T86YA=="
storage_account_name = "practice95"
connection_string = "DefaultEndpointsProtocol=https;AccountName=practice95;AccountKey=eyGvrinmFZ9/GsfhQxQkZRftMdi3A/2Xvp7PEK+B8NgiIXessYMTU6R4RU52F6hur8SHV1+TVDHr+ASt5T86YA==;EndpointSuffix=core.windows.net"
container_name = "dataset"

def uploadToBlobStorage(file_path,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
   with open(file_path,"rb") as data:
      blob_client.upload_blob(data)
      print(f"Uploaded {file_name}.")

# calling a function to perform upload
uploadToBlobStorage(r"D:\Datasets\autos_dataset.csv",'FILE_NAME')