#include "../inc/GCS_Persistent.h"

#ifdef GCS

void Init_GCS_client(::google::cloud::storage::Client &gcs_client, const std::string &bucket_name, std::string gcs_project_id, std::string location)
{
    // Initializes GCS client
    // Note: For local test, path to service account json file needs to be exported as environment variable.
    gcs_client = ::google::cloud::storage::Client();
    
    // Create bucket
    auto bucket_metadata = gcs_client.CreateBucketForProject(bucket_name,gcs_project_id,::google::cloud::storage::BucketMetadata().set_location(location));
    
    // Check if bucket was created.
    if (!bucket_metadata) 
        std::cout << "[GCS_INIT] status: " << bucket_metadata.status().message();
    else
        std::cout << "[GCS_INIT] Bucket: " << bucket_metadata->name() << std::endl;
}

void GCS_Get(::google::cloud::storage::Client &gcs_client, const std::string &bucket_name, const std::string &key, std::string &value)
{
    auto reader = gcs_client.ReadObject(bucket_name, key);

    // Check if reader fails.
    if (!reader)
        std::cout << "[GCS_READER] status: " << reader.status() << std::endl;
    else
    {
        std::string value_read{std::istreambuf_iterator<char>{reader}, {}};
        std::cout << "[GCS_READER] value: " << value_read << std::endl;
    }
}

void GCS_Put(::google::cloud::storage::Client &gcs_client, std::string &bucket_name, const std::string &key, const std::string &value)
{
    // Create stream object to write serialized data to.
    auto writer = gcs_client.WriteObject(bucket_name, key);
    
    // Write data to stream.
    writer << value;
    
    writer.Close();

    // Check if writer failed.
    if (!writer.metadata())
        std::cout << "[GCS_WRITER] status: " << writer.metadata().status() << std::endl;
    else 
        std::cout << "[GCS_WRITER] created key: " << key << std::endl;
    
    std::string test;
    GCS_Get(gcs_client, bucket_name, key, test);
}
#endif