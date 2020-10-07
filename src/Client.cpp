//
// Created by shahrooz on 9/15/20.
//

#include "../inc/Client.h"


Client::Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
        uint32_t timeout_per_request, std::vector<DC*>& datacenters) : id(id), local_datacenter_id(local_datacenter_id),
        retry_attempts(retry_attempts), metadata_server_timeout(metadata_server_timeout),
        timeout_per_request(timeout_per_request), datacenters(datacenters){

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < datacenters.size(); datacenter_indx++){
        if(datacenters[datacenter_indx]->id == local_datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    if(!datacenter_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong local_datacenter_id %d.\n", local_datacenter_id);
    }
    assert(datacenter_indx_found);

//    DPRINTF(DEBUG_CAS_Client, "AAAAAA local_datacenter_id %d.\n", local_datacenter_id);

    this->metadata_server_ip = datacenters[datacenter_indx]->metadata_server_ip;
    this->metadata_server_port = std::to_string(datacenters[datacenter_indx]->metadata_server_port);
//    DPRINTF(DEBUG_CAS_Client, "AAAA datacenter port: %s\n", metadata_server_port.c_str());
}

Client::~Client(){
}

const uint32_t& Client::get_id() const{
    return id;
}

const uint32_t& Client::get_local_datacenter_id() const{
    return local_datacenter_id;
}

const uint32_t& Client::get_retry_attempts() const{
    return retry_attempts;
}

const uint32_t& Client::get_metadata_server_timeout() const{
    return metadata_server_timeout;
}

const uint32_t& Client::get_timeout_per_request() const{
    return timeout_per_request;
}

const std::string& Client::get_metadata_server_ip() const{
    return metadata_server_ip;
}

const std::string& Client::get_metadata_server_port() const{
    return metadata_server_port;
}
