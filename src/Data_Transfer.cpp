#include "../inc/Data_Transfer.h"
#include <cstring>

int DataTransfer::sendAll(int sock, const void* data, int data_size){
    
    const char* iter = static_cast<const char*>(data);
    int bytes_sent = 0;

    while(data_size > 0){
        if((bytes_sent = send(sock, iter, data_size, 0)) < 1){
            if(bytes_sent == -1){
                DPRINTF(DEBUG_CAS_Client, "WARN: error in host socket.\n");
            }
            else if(bytes_sent == 0){
                DPRINTF(DEBUG_CAS_Client, "WARN: error in remote socket.\n");
            }
            return bytes_sent;
        }
        data_size -= bytes_sent;
        iter += bytes_sent;
    }
    
    return 1;
}

std::string DataTransfer::serialize(const strVec& data){
    
    packet::msg enc;
    for(auto it : data){
        enc.add_value(it);
    }
    std::string out_str;
    enc.SerializeToString(&out_str);
    return out_str;
}


//Called after socket connection established
//sends it across the socket
// Return 1 on SUCCESS
int DataTransfer::sendMsg(int sock, const std::string& out_str){

    assert(!out_str.empty());
    uint32_t _size = htonl(out_str.size());

    int result = sendAll(sock, &_size, sizeof(_size));
    if(result == 1){
        result = sendAll(sock, out_str.c_str(), out_str.size());
    }
    return result;
}

int DataTransfer::recvAll(int sock, void* buf, int data_size){
    
    char* iter = static_cast<char*>(buf);
    int bytes_read = 0;
    
    while(data_size > 0){
        if((bytes_read = recv(sock, iter, data_size, 0)) < 1){
            if(bytes_read == -1){
                DPRINTF(DEBUG_CAS_Client, "WARN: error in host socket.\n");
            }
            else if(bytes_read == 0){
                DPRINTF(DEBUG_CAS_Client, "WARN: error in remote socket.\n");
            }
            return bytes_read;
        }
        iter += bytes_read;
        data_size -= bytes_read;
    }
    
    return 1;
}

strVec DataTransfer::deserialize(const std::string& data){
    
    packet::msg dec;
    strVec out_data;
    
    dec.ParseFromString(data);
    
    int val_size = dec.value_size();
    
    for(int i = 0; i < val_size; i++){
        out_data.push_back(dec.value(i));
    }
    
    return out_data;
}

//Pass an empty Vector to it
// Decodes the msg and populates the vector
//Returns 1 on SUCCESS
int DataTransfer::recvMsg(int sock, std::string& data){
    
    uint32_t _size = 0;
    int result;
    result = recvAll(sock, &_size, sizeof(_size));
    if(result == 1){
        _size = ntohl(_size);
        if(_size > 0){
            data.resize(_size);
            result = recvAll(sock, const_cast<char*>(data.c_str()), _size);
            if(result != 1){
                data.clear();
                return result;
            }
        }
    }

    if(data.empty()){
        return -2;
    }
    
    return 1;
}

int DataTransfer::recvMsg_async(const int sock, std::promise <std::string>&& data_set){
    std::string data_temp;
    int result = DataTransfer::recvMsg(sock, data_temp);
    
    if(result == 1){
//        DPRINTF(DEBUG_UTIL, "result is 1\n");
        data_set.set_value(data_temp);
    }
    else{
        DPRINTF(DEBUG_UTIL, "ERROR: result is %d\n", result);
        data_temp = "ERROR";
        data_set.set_value(data_temp);
    }
    
    return result;
}

//std::string DataTransfer::serializePrp(const Properties& properties_p){
//
//    packet::properties prp;
//    prp.set_local_datacenter_id(properties_p.local_datacenter_id);
//    prp.set_retry_attempts(properties_p.retry_attempts);
//    prp.set_metadata_server_timeout(properties_p.metadata_server_timeout);
//    prp.set_timeout_per_request(properties_p.timeout_per_request);
//    prp.set_start_time(properties_p.start_time);
//
//    for(auto dc: properties_p.datacenters){
//        packet::Datacenter* DC = prp.add_datacenters();
//        DC->set_id(dc->id);
//        DC->set_metadata_server_ip(dc->metadata_server_ip);
//        DC->set_metadata_server_port(dc->metadata_server_port);
//
//        for(auto server: dc->servers){
//            packet::Server* sv = DC->add_servers();
//            sv->set_id(server->id);
//            sv->set_ip(server->ip);
//            sv->set_port(server->port);
//        }
//    }
//
//    for(auto grp: properties_p.groups){
//        packet::Group* group = prp.add_groups();
//        group->set_timestamp(grp->timestamp);
//
//        for(auto id: grp->grp_id){
//            group->add_grp_id(id);
//        }
//
//        for(auto gconfig: grp->grp_config){
//            packet::GroupConfig* grp_cfg = group->add_grp_config();
//            grp_cfg->set_object_size(gconfig->object_size);
//            grp_cfg->set_num_objects(gconfig->num_objects);
//            grp_cfg->set_arrival_rate(gconfig->arrival_rate);
//            grp_cfg->set_read_ratio(gconfig->read_ratio);
//            grp_cfg->set_duration(gconfig->duration);
//
//            for(uint i = 0; i < gconfig->keys.size(); i++){
//                grp_cfg->add_keys(gconfig->keys[i]);
//            }
//
//            for(uint i = 0; i < gconfig->client_dist.size(); i++){
//                grp_cfg->add_client_dist(gconfig->client_dist[i]);
//            }
//
//            Placement* pp = gconfig->placement_p;
//            packet::Placement* placement_p = grp_cfg->mutable_placement_p();
//            placement_p->set_protocol(pp->protocol);
//            placement_p->set_m(pp->m);
//            placement_p->set_k(pp->k);
//            placement_p->set_f(pp->f);
//
//            for(auto s : pp->servers){
//                placement_p->add_servers(s);
//            }
//
//            for(auto q : pp->Q1){
//                placement_p->add_q1(q);
//            }
//
//            for(auto q : pp->Q2){
//                placement_p->add_q2(q);
//            }
//
//            for(auto q : pp->Q3){
//                placement_p->add_q3(q);
//            }
//
//            for(auto q : pp->Q4){
//                placement_p->add_q4(q);
//            }
//
//        }
//
//    }
//
//    std::string str_out;
//    if(!prp.SerializeToString(&str_out)){
//        throw std::logic_error("Failed to serialize the message ! ");
//    }
//
//    return str_out;
//}
//
//std::string DataTransfer::serializePlacement(const Placement& placement){
//
//    packet::Placement p;
//
//    p.set_protocol(placement.protocol);
//    p.set_m(placement.m);
//    p.set_k(placement.k);
//    p.set_f(placement.f);
//
//    for(auto s : placement.servers){
//        p.add_servers(s);
//    }
//
//    for(auto q : placement.Q1){
//        p.add_q1(q);
//    }
//
//    for(auto q : placement.Q2){
//        p.add_q2(q);
//    }
//
//    for(auto q : placement.Q3){
//        p.add_q3(q);
//    }
//
//    for(auto q : placement.Q4){
//        p.add_q4(q);
//    }
//
//    std::string str_out;
//    if(!p.SerializeToString(&str_out)){
//        throw std::logic_error("Failed to serialize the message ! ");
//    }
//
//    return str_out;
//}
//
//Properties* DataTransfer::deserializePrp(std::string& data){
//
//    Properties* prp = new Properties;
//    packet::properties gprp;        // Nomenclature: add 'g' in front of gRPC variables
//    if(!gprp.ParseFromString(data)){
//        throw std::logic_error("Failed to Parse the input received ! ");
//    }
//
//    prp->local_datacenter_id = gprp.local_datacenter_id();
//    prp->retry_attempts = gprp.retry_attempts();
//    prp->metadata_server_timeout = gprp.metadata_server_timeout();
//    prp->timeout_per_request = gprp.timeout_per_request();
//    prp->start_time = gprp.start_time();
//
//    // Datacenter need to be inserted in order
//    for(int i = 0; i < gprp.datacenters_size(); i++){
//        DC* dc = new DC;
//        const packet::Datacenter& datacenter = gprp.datacenters(i);
//        dc->id = datacenter.id();
//        dc->metadata_server_ip = datacenter.metadata_server_ip();
//        dc->metadata_server_port = datacenter.metadata_server_port();
//
//        for(int j = 0; j < datacenter.servers_size(); j++){
//            Server* sv = new Server;
//            const packet::Server& server = datacenter.servers(j);
//            sv->id = server.id();
//            sv->ip = server.ip();
//            sv->port = server.port();
//            sv->datacenter = dc;
//            dc->servers.push_back(sv);
//        }
//
//        prp->datacenters.push_back(dc);
//    }
//
//
//    for(int i = 0; i < gprp.groups_size(); i++){
//        Group* grp = new Group;
//        const packet::Group& ggrp = gprp.groups(i);
//
//        grp->timestamp = ggrp.timestamp();
//
//        grp->id = ggrp.id();
//
//        for(int j = 0; j < ggrp.grp_id_size(); j++){
//            grp->grp_id.push_back(ggrp.grp_id(j));
//        }
//
//        for(int j = 0; j < ggrp.grp_config_size(); j++){
//            GroupConfig* gcfg = new GroupConfig;
//            const packet::GroupConfig& ggcfg = ggrp.grp_config(j);
//
//            gcfg->object_size = ggcfg.object_size();
//            gcfg->num_objects = ggcfg.num_objects();
//            gcfg->arrival_rate = ggcfg.arrival_rate();
//            gcfg->read_ratio = ggcfg.read_ratio();
//            gcfg->duration = ggcfg.duration();
//
//            for(int m = 0; m < ggcfg.keys_size(); m++){
//                gcfg->keys.push_back(ggcfg.keys(m));
//            }
//
//            for(int m = 0; m < ggcfg.client_dist_size(); m++){
//                gcfg->client_dist.push_back(ggcfg.client_dist(m));
//            }
//
//            Placement* plc = new Placement;
//            const packet::Placement& gplc = ggcfg.placement_p();
//
//            plc->protocol = gplc.protocol();
//            plc->m = gplc.m();
//            plc->k = gplc.k();
//            plc->f = gplc.f();
//
//            for(int m = 0; m < gplc.servers_size(); m++){
//                plc->servers.push_back(gplc.servers(m));
//            }
//
//            for(int m = 0; m < gplc.q1_size(); m++){
//                plc->Q1.push_back(gplc.q1(m));
//            }
//            for(int m = 0; m < gplc.q2_size(); m++){
//                plc->Q2.push_back(gplc.q2(m));
//            }
//            for(int m = 0; m < gplc.q3_size(); m++){
//                plc->Q3.push_back(gplc.q3(m));
//            }
//            for(int m = 0; m < gplc.q4_size(); m++){
//                plc->Q4.push_back(gplc.q4(m));
//            }
//            gcfg->placement_p = plc;
//
//            grp->grp_config.push_back(gcfg);
//        }
//
//        prp->groups.push_back(grp);
//    }
//
//    return prp;
//}
//
//Placement* DataTransfer::deserializePlacement(const std::string& data){
//    Placement* p = new Placement;
//    packet::Placement gp;        // Nomenclature: add 'g' in front of gRPC variables
//    if(!gp.ParseFromString(data)){
//        throw std::logic_error("Failed to Parse the input received ! ");
//    }
//
//    p->protocol = gp.protocol();
//    p->m = gp.m();
//    p->k = gp.k();
//    p->f = gp.f();
//
//    for(int m = 0; m < gp.servers_size(); m++){
//        p->servers.push_back(gp.servers(m));
//    }
//    for(int m = 0; m < gp.q1_size(); m++){
//        p->Q1.push_back(gp.q1(m));
//    }
//    for(int m = 0; m < gp.q2_size(); m++){
//        p->Q2.push_back(gp.q2(m));
//    }
//    for(int m = 0; m < gp.q3_size(); m++){
//        p->Q3.push_back(gp.q3(m));
//    }
//    for(int m = 0; m < gp.q4_size(); m++){
//        p->Q4.push_back(gp.q4(m));
//    }
//
//    return p;
//}
//
//std::string DataTransfer::serializeCFG(const Placement& pp){
//
//    packet::Placement placement_p;
//    placement_p.set_protocol(pp.protocol);
//    placement_p.set_m(pp.m);
//    placement_p.set_k(pp.k);
//    placement_p.set_f(pp.f);
//
//    for(auto s : pp.servers){
//        placement_p.add_servers(s);
//    }
//
//    for(auto q : pp.Q1){
//        placement_p.add_q1(q);
//    }
//
//    for(auto q : pp.Q2){
//        placement_p.add_q2(q);
//    }
//
//    for(auto q : pp.Q3){
//        placement_p.add_q3(q);
//    }
//
//    for(auto q : pp.Q4){
//        placement_p.add_q4(q);
//    }
//
//    std::string str_out;
//    if(!placement_p.SerializeToString(&str_out)){
//        throw std::logic_error("Failed to serialize the message ! ");
//    }
//
//    return str_out;
//}
//
//
//Placement DataTransfer::deserializeCFG(std::string& data){
//
//    Placement plc;
//    packet::Placement gplc;
//
//    if(!gplc.ParseFromString(data)){
//        throw std::logic_error("Failed to Parse the input received ! ");
//    }
//
//    plc.protocol = gplc.protocol();
//    plc.m = gplc.m();
//    plc.k = gplc.k();
//    plc.f = gplc.f();
//
//    for(int m = 0; m < gplc.servers_size(); m++){
//        plc.servers.push_back(gplc.servers(m));
//    }
//    for(int m = 0; m < gplc.q1_size(); m++){
//        plc.Q1.push_back(gplc.q1(m));
//    }
//    for(int m = 0; m < gplc.q2_size(); m++){
//        plc.Q2.push_back(gplc.q2(m));
//    }
//    for(int m = 0; m < gplc.q3_size(); m++){
//        plc.Q3.push_back(gplc.q3(m));
//    }
//    for(int m = 0; m < gplc.q4_size(); m++){
//        plc.Q4.push_back(gplc.q4(m));
//    }
//
//    return plc;
//}

std::string DataTransfer::serializeMDS(const std::string& status, const std::string& msg, const std::string& key,
                                       const uint32_t& curr_conf_id, const uint32_t& new_conf_id, const std::string& timestamp,
                                       const Placement& placement){
    packet::MetaDataServer mds;
    mds.set_status(status);
    mds.set_msg(msg);
    mds.set_key(key);
    mds.set_curr_conf_id(curr_conf_id);
    mds.set_new_conf_id(new_conf_id);
    mds.set_timestamp(timestamp);
    
//    if(placement != nullptr){
    packet::Placement* mds_placement = mds.mutable_placement();
//    const Placement& pp = *placement;

    mds_placement->set_protocol(placement.protocol);
    mds_placement->set_m(placement.m);
    mds_placement->set_k(placement.k);
    mds_placement->set_f(placement.f);

    for(auto s: placement.servers){
        mds_placement->add_servers(s);
    }

    for(auto& quo: placement.quorums){
        packet::Quorums* quorums = mds_placement->add_quorums();
        for(auto q : quo.Q1){
            quorums->add_q1(q);
        }

        for(auto q : quo.Q2){
            quorums->add_q2(q);
        }

        for(auto q : quo.Q3){
            quorums->add_q3(q);
        }

        for(auto q : quo.Q4){
            quorums->add_q4(q);
        }
    }
//    }
//    else{
//        packet::Placement* placement_p = mds.mutable_placement_p();
//
//        placement_p->set_protocol("");
//        placement_p->set_m(0);
//        placement_p->set_k(0);
//        placement_p->set_f(0);
//    }
    
    std::string str_out;
    if(!mds.SerializeToString(&str_out)){
        throw std::logic_error("Failed to serialize the message ! ");
    }
    return str_out;
}

std::string DataTransfer::serializeMDS(const std::string& status, const std::string& msg, const std::string& key,
                                const uint32_t& curr_conf_id, const uint32_t& new_conf_id, const std::string& timestamp){
    Placement p;
    return DataTransfer::serializeMDS(status, msg, key, curr_conf_id, new_conf_id, timestamp, p);
}

Placement DataTransfer::deserializeMDS(const std::string& data, std::string& status, std::string& msg, std::string& key,
                                        uint32_t& curr_conf_id, uint32_t& new_conf_id, std::string& timestamp){
    Placement p;
    packet::MetaDataServer mds;        // Nomenclature: add 'g' in front of gRPC variables
    if(!mds.ParseFromString(data)){
        throw std::logic_error("Failed to Parse the input received ! ");
    }
    
    status = mds.status();
    msg = mds.msg();
    key = mds.key();
    curr_conf_id = mds.curr_conf_id();
    new_conf_id = mds.new_conf_id();
    timestamp = mds.timestamp();
    
//    if(!(status == "ERROR" || status == "WARN" || (status == "OK" && msg == "key updated"))){
//    p = new Placement;

    const packet::Placement& gp = mds.placement();

    p.protocol = gp.protocol();
    p.m = gp.m();
    p.k = gp.k();
    p.f = gp.f();

    for(auto s: gp.servers()){
        p.servers.push_back(s);
    }

    for(auto& quo: gp.quorums()){
        p.quorums.emplace_back();
        for(auto q: quo.q1()){
            p.quorums.back().Q1.push_back(q);
        }
        for(auto q: quo.q2()){
            p.quorums.back().Q2.push_back(q);
        }
        for(auto q: quo.q3()){
            p.quorums.back().Q3.push_back(q);
        }
        for(auto q: quo.q4()){
            p.quorums.back().Q4.push_back(q);
        }
    }

//    }
    
    return p;
}

Placement DataTransfer::deserializeMDS(const std::string& data, std::string& status, std::string& msg){
    std::string key;
    uint32_t curr_conf_id;
    uint32_t new_conf_id;
    std::string timestamp;
    return DataTransfer::deserializeMDS(data, status, msg, key, curr_conf_id, new_conf_id, timestamp);
}