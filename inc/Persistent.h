#ifndef _PERSISTENT_H_
#define _PERSISTENT_H_

#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <cassert>
#include "Util.h"
#include "../inc/GCS_Persistent.h"

class Persistent{

public:
    Persistent(const std::string& directory);
    Persistent(const Persistent& orig) = delete;
    ~Persistent();

    std::vector<std::string> get(const std::string& key);
    void put(const std::string& key, const std::vector<std::string>& value);
    void merge(const std::string& key, const std::vector<std::string>& value);

private:
    rocksdb::DB* db;
    #ifdef GCS
    // gcs Client
    ::google::cloud::storage::Client gcs_client;
    // Directory is used as bucket name.
    std::string bucket_name;
    #endif
};


#endif
