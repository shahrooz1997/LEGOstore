//
// Created by shahrooz on 9/15/20.
//

#ifndef LEGOSTORE_CLIENT_H
#define LEGOSTORE_CLIENT_H

#include <stdint.h>
#include <string>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "Timestamp.h"
#include "Data_Transfer.h"
#include <map>
#include <utility>
#include <sys/time.h>
#include <future>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>
#include "Util.h"

// Todo: get rid of this class and use all its attributes in the Client_Node class

class Key_gaurd;

class Client{
public:
    Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters);
    
    Client(const Client& orig) = delete;
    
    virtual ~Client();
    
    virtual int put(const std::string& key, const std::string& value) = 0;
    
    virtual int get(const std::string& key, std::string& value) = 0;

    const uint32_t& get_id() const;

    const uint32_t& get_local_datacenter_id() const;

    const uint32_t& get_retry_attempts() const;

    const uint32_t& get_metadata_server_timeout() const;

    const uint32_t& get_timeout_per_request() const;

    const std::string& get_metadata_server_ip() const;

    const std::string& get_metadata_server_port() const;

protected:
    uint32_t id;
    uint32_t local_datacenter_id;
    uint32_t retry_attempts;
    uint32_t metadata_server_timeout;
    uint32_t timeout_per_request;
//    uint64_t                start_time;
    std::vector<DC*>& datacenters; // Should not be deleted in Client destructor
    std::string current_class;
    std::string metadata_server_ip;
    std::string metadata_server_port;


    // Handling several operations on different keys simultaneously
    std::vector<std::string> ongoing_keys;
    std::mutex ongoing_keys_lock;
    std::condition_variable ongoing_keys_cv;
    void check_and_add_ongoing_keys(const std::string& key); // It will block the caller thread until the key is removed from ongoing_keys
    void remove_ongoing_keys(const std::string& key);
    friend class Key_gaurd;
};

class Key_gaurd{
public:
    Key_gaurd(Client* client_p, const std::string& key);
    Key_gaurd(const Key_gaurd& orig) = delete;
    virtual ~Key_gaurd();

private:
    Client* client_p;
    const std::string& key;
};

#endif //LEGOSTORE_CLIENT_H
