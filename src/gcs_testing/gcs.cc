// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*

This code writes to a file in a bucket, reads it, overwrites it and reads it again to check if it works.

*/

#include "../../inc/google/cloud/storage/client.h"
#include <iostream>

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Missing bucket name.\n";
    std::cerr << "Usage: quickstart <bucket-name>\n";
    return 1;
  }
  std::string const bucket_name = argv[1];

  // Create aliases to make the code easier to read.
  namespace gcs = ::google::cloud::storage;

  // Create a client to communicate with Google Cloud Storage. This client
  // uses the default configuration for authentication and project id.
  auto client = gcs::Client();

  auto metadata_status = client.CreateBucketForProject(bucket_name,"personal-use-316018",gcs::BucketMetadata().set_location("us-east1"));
  if(!metadata_status)
    std::cout << metadata_status.status() << std::endl;

  auto writer = client.WriteObject(bucket_name, "20!CAS!1!201850887-4");
  writer << "Hello World!";
  writer.Close();
  if (writer.metadata()) {
    std::cout << "Successfully created object: " << *writer.metadata() << "\n";
  } else {
    std::cerr << "Error creating object: " << writer.metadata().status()
              << "\n";
    return 1;
  }

  auto reader = client.ReadObject(bucket_name, "20!CAS!1!201850887-4");
  if (!reader) {
    std::cerr << "Error reading object: " << reader.status() << "\n";
    return 1;
  }

  std::string contents{std::istreambuf_iterator<char>{reader}, {}};
  std::cout << contents << "\n";

  auto writer1 = client.WriteObject(bucket_name, "20!CAS!1!201850887-4");
  writer1 << "Ciao";
  writer1.Close();
  if (writer1.metadata()) {
    std::cout << "Successfully created object: " << *writer1.metadata() << "\n";
  } else {
    std::cerr << "Error creating object: " << writer1.metadata().status()
              << "\n";
    return 1;
  }

  auto reader1 = client.ReadObject(bucket_name, "20!CAS!1!201850887-4");
  if (!reader1) {
    std::cerr << "Error reading object: " << reader1.status() << "\n";
    return 1;
  }
  std::string contents1{std::istreambuf_iterator<char>{reader1}, {}};
  std::cout << contents1 << "\n";


  return 0;
}
