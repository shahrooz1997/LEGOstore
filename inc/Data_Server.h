#ifndef DATA_SERVER_H
#define DATA_SERVER_H

#include "Cache.h"
#include "Persistent.h"
#include "CAS_Server.h"
#include "ABD_Server.h"
#include <unordered_map>
#include <mutex>
#include <memory>
#include <condition_variable>
#include "Timestamp.h"

typedef std::vector <std::string> strVec;

class DataServer{

public:
    // TODO:: removed the null value of socket, directly intializing class variable
    
    DataServer(std::string directory, int sock, std::string metadata_server_ip = "127.0.0.1", std::string metadata_server_port = "30000");
    
    int getSocketDesc();
    
    std::string get_timestamp(const std::string& key, const std::string& curr_class, uint32_t conf_id);
    
    std::string put(const std::string& key, const std::string& value, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id);
    
    std::string put_fin(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id);
    
    std::string get(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id);

    // Reconfig queries
    std::string reconfig_query(const std::string& key, const std::string& curr_class, uint32_t conf_id);
    std::string reconfig_finalize(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id);
    std::string reconfig_write(const std::string& key, const std::string& value, const std::string& timestamp, const std::string& curr_class,
            uint32_t conf_id);
    std::string finish_reconfig(const std::string &key, const std::string &timestamp, const std::string& new_conf_id, const std::string &curr_class, uint32_t conf_id);

private:
    int sockfd;
    std::vector<std::unique_ptr<std::mutex>> mu_p_vec;
    Cache cache;
    Persistent persistent;
    CAS_Server CAS;
    ABD_Server ABD;
    
    std::string metadata_server_ip;
    std::string metadata_server_port;

    strVec get_data(const std::string& key);
    int put_data(const std::string& key, const strVec& value);

    // Handling block mode of keys
    std::unordered_set<std::string> blocked_keys;
    std::mutex blocked_keys_lock;
    std::condition_variable blocked_keys_cv;
    void check_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id); // It will block the caller thread until the key is removed from the recon state
    void add_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id);
    void remove_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id);
};

//class Persistent_merge : public rocksdb::AssociativeMergeOperator{
//public:
//    virtual bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const rocksdb::Slice& value, std::string* new_value,
//                       rocksdb::Logger* logger) const override;
//
//    virtual const char* Name() const override{ return "Persistent_merge"; }
//
//};

#endif
