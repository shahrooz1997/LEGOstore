#include "gcs_config.h"
#include "google/cloud/storage/client.h"

#ifdef GCS

void Init_GCS_client(::google::cloud::storage::Client &gcs_client, const std::string &bucket_name, std::string gcs_project_id, std::string location);

void GCS_Get(::google::cloud::storage::Client &gcs_client, const std::string &bucket_name, const std::string &key, std::string &value);

void GCS_Put(::google::cloud::storage::Client &gcs_client, std::string &bucket_name, const std::string &key, const std::string &value);

#endif