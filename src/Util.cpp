/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Util.cpp
 * Author: shahrooz
 *
 * Created on January 18, 2020, 5:18 PM
 */

// for open/close pair
#include "Util.h"
#include "../inc/Util.h"
#include "Data_Transfer.h"
#include <cstring>
#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/tcp.h>

#ifdef DEVELOPMENT
bool DEBUG_CAS_Client = true;
bool DEBUG_ABD_Client = true;
bool DEBUG_CAS_Server = true;
bool DEBUG_ABD_Server = true;
bool DEBUG_RECONFIG_CONTROL = true;
#else
bool DEBUG_CLIENT_NODE = true;
bool DEBUG_CAS_Client = true;
bool DEBUG_ABD_Client = true;
bool DEBUG_CAS_Server = true;
bool DEBUG_ABD_Server = true;
bool DEBUG_RECONFIG_CONTROL = true;
bool DEBUG_METADATA_SERVER = true;
bool DEBUG_UTIL = true;
#endif


JSON_Reader::JSON_Reader(){
}

JSON_Reader::~JSON_Reader(){
}

void print_time(){
    auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << ctime(&timenow) << std::endl;
    
}

std::string convert_ip_to_string(uint32_t ip){
    ip = htonl(ip);
    unsigned char* p = (unsigned char*)(&ip);
    std::string ret;
    for(int i = 0; i < 4; i++){
        ret += std::to_string(*(p++));
        if(i != 3){
            ret += '.';
        }
    }
    return ret;
}

// Used for creating server sockets
// Returns the socket FD after creation, binding and listening
// nullptr in IP implies the use of local IP
int socket_setup(const std::string& port, const std::string* IP){
    
    struct addrinfo hint, * res, * ptr;
    int status = 0, enable = 1;
    int socketfd;
    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_INET;
    hint.ai_socktype = SOCK_STREAM;
    
    if(IP == nullptr){
        hint.ai_flags = AI_PASSIVE;
        status = getaddrinfo(NULL, port.c_str(), &hint, &res);
    }
    else{
        status = getaddrinfo((*IP).c_str(), port.c_str(), &hint, &res);
    }
    
    if(status != 0){
        fprintf(stdout, "getaddrinfo: %s\n", gai_strerror(status));
        assert(0);
    }
    
    // Loop through all the options, unless one succeeds
    for(ptr = res; res != NULL; ptr = ptr->ai_next){
        
        if((socketfd = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol)) == -1){
            perror("server -> socket");
            continue;
        }
        
        if(setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1){
            perror("server -> set socket options");
            continue;
        }
        
        // enable = 0;
        // uint size_enable = sizeof(enable);
        // getsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &enable, &size_enable);
        // printf(" Size of the receive buffer is %u\n", enable);
        //
        //
        // enable = 212992;
        // if( setsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &enable, sizeof(enable)) == -1){
        // 	perror("server -> set socket options");
        // 	continue;
        // }
        
        
        // struct linger sock_linger;
        // sock_linger.l_onoff = 1;
        // sock_linger.l_linger = MAX_LINGER_BEFORE_SOCK_CLOSE;
        // if( setsockopt(socketfd, SOL_SOCKET, SO_LINGER, &sock_linger, sizeof(sock_linger)) == -1){
        //     perror("server -> set socket options");
        //     continue;
        // }
        
        if(bind(socketfd, ptr->ai_addr, ptr->ai_addrlen) == -1){
            close(socketfd);
            perror("server -> bind");
            continue;
        }
        
        break;
    }
    
    freeaddrinfo(res);
    
    if(ptr == NULL){
        fprintf(stderr, "Socket failed to bind\n");
        assert(0);
    }
    
    if(listen(socketfd, BACKLOG) == -1){
        perror("server -> listen");
        assert(0);
    }
    
    return socketfd;
}

//Returns 0 on success
//int socket_cnt(int& sock, uint16_t port, const std::string& IP){
//
//    struct sockaddr_in serv_addr;
//    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
//        perror("\n Socket creation error \n");
//        return 1;
//    }
//    serv_addr.sin_family = AF_INET;
//    serv_addr.sin_port = htons(port);
//    std::string ip_str = IP;
//    //std::string ip_str = convert_ip_to_string(server->ip);
//
//    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
//        perror("\nInvalid address/ Address not supported \n");
//        return 1;
//    }
//
//    if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
//        perror("\nConnection Failed \n");
//        return 1;
//    }
//
//    return 0;
//}

//Returns 0 on success
//int client_cnt(int& sock, Server* server){
//
//    struct sockaddr_in serv_addr;
//    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
//        printf("\n Socket creation error \n");
//        return S_FAIL;
//    }
//    serv_addr.sin_family = AF_INET;
//    serv_addr.sin_port = htons(server->port);
//    std::string ip_str = server->ip;
//
//    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
//        printf("\nInvalid address/ Address not supported \n");
//        return S_FAIL;
//    }
//
//    if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
//        printf("\nConnection Failed \n");
//        return S_FAIL;
//    }
//
//    return S_OK;
//}

//std::map<std::string, int> Connect::socks;
//std::map<std::string, std::unique_ptr<std::mutex> > Connect::socks_lock;
//std::map<std::string, bool> Connect::is_sock_lock;
std::mutex Connect::init_lock;
std::vector<std::pair<std::string, int> > Connect::socks(1024);
std::vector<std::pair<std::string, std::unique_ptr<std::mutex> > > Connect::socks_lock(1024);
std::vector<std::pair<std::string, bool> > Connect::is_sock_lock(1024);
uint32_t Connect::number_of_socks = 0;

template <typename T, typename U>
int vecpair_find(const std::vector<std::pair<T, U> >& vec, const T& search_val, uint32_t number_of_socks = 1024){
    int ret = -1;
    for(uint i = 0; i < number_of_socks; i++){
        if(vec[i].first == search_val){
            ret = i;
        }
    }
    return ret;
}

Connect::Connect(const std::string& ip, const uint16_t port) : ip(ip), port(port), sock(0), connected(false){

    std::string ip_port = ip + "!" + std::to_string(port);
//    DPRINTF(DEBUG_UTIL, "tloed: IP: %s Port: %d\n", ip.c_str(), port);
    init_lock.lock();
//    DPRINTF(DEBUG_UTIL, "loied: IP: %s Port: %d\n", ip.c_str(), port);
    idx = vecpair_find(this->socks_lock, ip_port, number_of_socks);
    if(idx == -1){
        try {
            this->socks_lock[number_of_socks] = std::make_pair(ip_port, std::unique_ptr<std::mutex>(new std::mutex));
            this->socks[number_of_socks] = std::make_pair(ip_port, -1);
            this->is_sock_lock[number_of_socks] = std::make_pair(ip_port, false);
        }
        catch (std::bad_alloc &ba) {
            std::string err_msg = "bad_alloc caught: ";
            err_msg += ba.what();
            print_error(err_msg);
        }
        idx = number_of_socks;
        number_of_socks++;
//        DPRINTF(DEBUG_UTIL, "tloed: INIT: pointer is %p\n", this->socks_lock[idx].second.get());
    }
    init_lock.unlock();



//    std::string ip_port = ip + "!" + std::to_string(port);
//    DPRINTF(DEBUG_UTIL, "tloed: IP: %s Port: %d\n", ip.c_str(), port);
//    init_lock.lock();
//    DPRINTF(DEBUG_UTIL, "loied: IP: %s Port: %d\n", ip.c_str(), port);
//    if(this->socks_lock.find(ip_port) == this->socks_lock.end()){ // This if is necessary to improve performance
//        try {
//            this->socks_lock[ip_port] = std::unique_ptr<std::mutex>(new std::mutex);
//            this->socks[ip_port] = -1;
//            this->is_sock_lock[ip_port] = false;
//        }
//        catch (std::bad_alloc &ba) {
//            std::string err_msg = "bad_alloc caught: ";
//            err_msg += ba.what();
//            print_error(err_msg);
//        }
//        DPRINTF(DEBUG_UTIL, "tloed: INIT: pointer is %p\n", this->socks_lock[ip_port].get());
//    }
//    init_lock.unlock();
//    DPRINTF(DEBUG_UTIL, "tloed: pointer is %p\n", this->socks_lock[idx].second.get());
    this->socks_lock[idx].second.get()->lock();
//    DPRINTF(DEBUG_UTIL, "loed: IP: %s Port: %d\n", ip.c_str(), port);
    is_sock_lock[idx].second = true;

//    DPRINTF(DEBUG_UTIL, "afloed: IP_Port is: %s , and the value of this->is_sock_lock[ip_port] is %x\n", ip_port.c_str(), this->is_sock_lock[ip_port]);

    if(this->socks[idx].second == -1){
        bool error = false;
        struct sockaddr_in serv_addr;
        if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
            print_error("Socket creation error");
            error = true;
        }
    
        if(!error){
            int yes = 1;
            int result = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*) &yes, sizeof(int));
            if(result < 0){
                print_error("setsockopt error");
                error = true;
            }
        }
    
        if(!error){
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(this->port);
            std::string ip_str = ip;
        
            if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
                print_error("Invalid address/ Address not supported");
                error = true;
            }
        }
    
        if(!error){
            if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
                print_error("Connection Failed");
                error = true;
            }
        }
    
        if(!error){
            this->socks[idx].second = this->sock;
            connected = true;
        }
    }
    else{
        this->sock = this->socks[idx].second;
        this->connected = true;
    }
}

Connect::Connect(const std::string& ip, const std::string& port) : ip(ip), sock(0), connected(false){

    std::string ip_port = ip + "!" + port;
//    DPRINTF(DEBUG_UTIL, "tloed: IP: %s Port: %s\n", ip.c_str(), port.c_str());
    init_lock.lock();
//    DPRINTF(DEBUG_UTIL, "loied: IP: %s Port: %s\n", ip.c_str(), port.c_str());
    idx = vecpair_find(this->socks_lock, ip_port, number_of_socks);
    if(idx == -1){
        try {
            this->socks_lock[number_of_socks] = std::make_pair(ip_port, std::unique_ptr<std::mutex>(new std::mutex));
            this->socks[number_of_socks] = std::make_pair(ip_port, -1);
            this->is_sock_lock[number_of_socks] = std::make_pair(ip_port, false);
        }
        catch (std::bad_alloc &ba) {
            std::string err_msg = "bad_alloc caught: ";
            err_msg += ba.what();
            print_error(err_msg);
        }
        idx = number_of_socks;
        number_of_socks++;
//        DPRINTF(DEBUG_UTIL, "tloed: INIT: pointer is %p\n", this->socks_lock[idx].second.get());
    }
    init_lock.unlock();

//    DPRINTF(DEBUG_UTIL, "tloed: pointer is %p\n", this->socks_lock[idx].second.get());
    this->socks_lock[idx].second.get()->lock();
//    DPRINTF(DEBUG_UTIL, "loed: IP: %s Port: %s\n", ip.c_str(), port.c_str());
    is_sock_lock[idx].second = true;


//    std::string ip_port = ip + "!" + port;
//    DPRINTF(DEBUG_UTIL, "tloed2: IP: %s Port: %s\n", ip.c_str(), port.c_str());

//    if(this->socks_lock.find(ip_port) == this->socks_lock.end()){
//        init_lock.lock();
//        if(this->socks_lock.find(ip_port) == this->socks_lock.end()){ // This if is necessary to improve performance
//            try {
//                this->socks_lock[ip_port] = std::unique_ptr<std::mutex>(new std::mutex);
//                this->socks[ip_port] = -1;
//                this->is_sock_lock[ip_port] = false;
//            }
//            catch (std::bad_alloc &ba) {
//                std::string err_msg = "bad_alloc caught: ";
//                err_msg += ba.what();
//                print_error(err_msg);
//            }
//        }
//        init_lock.unlock();
//        DPRINTF(DEBUG_UTIL, "tloed: INIT: pointer is %p\n", this->socks_lock[ip_port].get());
////        DPRINTF(DEBUG_UTIL, "tloed: INIT: pointer is %p\n", mu_temp);
//    }
////    init_lock.unlock();
//    DPRINTF(DEBUG_UTIL, "tloed2: pointer is %p\n", this->socks_lock[ip_port].get());
////    DPRINTF(DEBUG_UTIL, "tloed2: pointer is %p\n", mu_temp);
//    this->socks_lock[ip_port].get()->lock();
//    DPRINTF(DEBUG_UTIL, "loed2: IP: %s Port: %s\n", ip.c_str(), port.c_str());
//    is_sock_lock[ip_port] = true;

    if(this->socks[idx].second == -1){
        // Convert string to uint16_t
        connected = false;
        bool error = false;
        char* end;
        errno = 0;
        intmax_t val = strtoimax(port.c_str(), &end, 10);
        if(errno == ERANGE || val < 0 || val > UINT16_MAX || end == port.c_str() || *end != '\0'){
            error = true;
        }
//        DPRINTF(DEBUG_UTIL, "loed: Port: %d\n", (uint16_t)val);
    
        struct sockaddr_in serv_addr;
    
        if(!error){
            this->port = (uint16_t)val;
            if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
                print_error("Socket creation error");
                error = true;
            }
        }
    
        if(!error){
            int yes = 1;
            int result = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&yes, sizeof(int));
            if(result < 0){
                print_error("setsockopt error");
                error = true;
            }
        }
    
        if(!error){
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(this->port);
            std::string ip_str = ip;
        
            if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
                print_error("Invalid address/ Address not supported");
                error = true;
            }
        }
    
        if(!error){
            if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
                print_error("Connection Failed");
                error = true;
            }
        }
    
        if(!error){
            this->socks[idx].second = this->sock;
            connected = true;
        }
    }
    else{
        char* end;
        intmax_t val = strtoimax(port.c_str(), &end, 10);
        this->port = (uint16_t)val;
        this->sock = this->socks[idx].second;
        this->connected = true;
    }
}

Connect::~Connect(){
//    ::close(sock);
//    DPRINTF(DEBUG_UTIL, "loed: destructor called. IP: %s Port: %d\n", ip.c_str(), port);
    this->unlock();
}

void Connect::unlock(){
    std::string ip_port = this->ip + "!" + std::to_string(port);
//    DPRINTF(DEBUG_UTIL, "unloed: IP_Port is: %s , and the value of this->is_sock_lock[ip_port] is %x\n", ip_port.c_str(), this->is_sock_lock[idx].second);
    if(this->is_sock_lock[idx].second) {
        this->is_sock_lock[idx].second = false;
//        DPRINTF(DEBUG_UTIL, "unloed: IP: %s Port: %d\n", ip.c_str(), port);
        this->socks_lock[idx].second.get()->unlock();
    }
}

std::string Connect::get_ip(){
    return ip;
}

uint16_t Connect::get_port(){
    return port;
}

bool Connect::is_connected(){
    return connected;
}

int Connect::operator*(){
    if(is_connected()){
        return this->sock;
    }
    else{
        print_error("Trying to get the fd of a socket which is not connected.");
        return -1;
    }
}

//void Connect::close(){
//    ::close(sock);
//}

void Connect::close_all(){
    for(auto it = socks.begin(); it != socks.end(); it++){
        ::close(it->second);
    }
}

void Connect::print_error(std::string const& m){
    std::stringstream msg; // Make it thread safe
    msg << "ip=" << this->ip << ", port=" << this->port << ": ERROR SOCKET CONNECTION, ";
    msg << m << std::endl;
    std::cout << msg.str();
}

Server::Server(){
    this->id = -1;
    this->port = 0;
    this->datacenter = nullptr;
}

Server::Server(const Server& orig){
    this->id = orig.id;
    this->ip = orig.ip;
    this->port = orig.port;
    this->datacenter = orig.datacenter;
}

Datacenter::Datacenter(){
    this->id = -1;
    this->metadata_server_port = 0;
}

Datacenter::Datacenter(const Datacenter& orig){
    this->id = orig.id;
    this->metadata_server_ip = orig.metadata_server_ip;
    this->metadata_server_port = orig.metadata_server_port;
    for(auto s: orig.servers){
        Server* s_temp = new Server(*s);
        s_temp->datacenter = this;
        this->servers.push_back(s_temp);
    }
}

Datacenter::~Datacenter(){
    for(auto& it: servers) {
        if (it != nullptr){
            delete it;
            it = nullptr;
        }
    }
    this->servers.clear();
}

Placement::Placement(){
    m = -1;
    k = -1;
    f = -1;
}

WorkloadConfig::~WorkloadConfig(){
    for(auto& it: grp) {
        if(it != nullptr){
            delete it;
            it = nullptr;
        }
    }
    this->grp.clear();
}

GroupConfig::GroupConfig(){
    object_size = 0;
    num_objects = 0;
    arrival_rate = 0; // Combined arrival rate
    read_ratio = 0;
    duration = 0;
    placement_p = nullptr;
}

GroupConfig::GroupConfig(const GroupConfig& orig){
    object_size = orig.object_size;
    num_objects = orig.num_objects;
    arrival_rate = orig.arrival_rate; // Combined arrival rate
    read_ratio = orig.read_ratio;
    duration = orig.duration;
    keys = orig.keys;
    client_dist = orig.client_dist;
    placement_p = new Placement(*(orig.placement_p));
}

GroupConfig::~GroupConfig(){
    if(placement_p != nullptr){
        delete placement_p;
        placement_p = nullptr;
    }
}

Group::Group(){
    timestamp = -1;
}

Group::Group(const Group& orig){
    timestamp = orig.timestamp;
    grp_id = orig.grp_id;
    for(auto gc: orig.grp_config){
        this->grp_config.push_back(new GroupConfig(*(gc)));
    }
}

Group::~Group(){
    for(auto& it: grp_config) {
        if(it != nullptr){
            delete it;
            it = nullptr;
        }
    }
    this->grp_config.clear();
}

Properties::~Properties(){
    for(auto& it: datacenters) {
        if(it != nullptr){
            delete it;
            it = nullptr;
        }
    }
    this->datacenters.clear();
    for(auto& it: groups) {
        if(it != nullptr){
            delete it;
            it = nullptr;
        }
    }
    this->groups.clear();
}

Reconfig_key_info::Reconfig_key_info(){
    curr_conf_id = -1;
    curr_placement = nullptr;
    reconfig_state = 0;
    timestamp = "";
    next_conf_id = -1;
    next_placement = nullptr;
}

Reconfig_key_info::Reconfig_key_info(const std::string& in){
    uint32_t cc = 0;

//    DPRINTF(DEBUG_UTIL, "111111\n");
    
    if(cc + sizeof(uint32_t) > in.size()){
        DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
    }
    for(uint32_t i = 0; i < sizeof(uint32_t) && cc < in.size(); i++){
        ((char*)(&(this->curr_conf_id)))[i] = in[cc++];
    }

//    DPRINTF(DEBUG_UTIL, "curr_conf_id is %u\n", curr_conf_id);

//    DPRINTF(DEBUG_UTIL, "22222\n");
    
    // current placement
    uint32_t temp = 0;
    if(cc + sizeof(uint32_t) > in.size()){
        DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
    }
    for(uint32_t i = 0; i < sizeof(uint32_t) && cc < in.size(); i++){
        ((char*)(&(temp)))[i] = in[cc++];
    }
//    DPRINTF(DEBUG_UTIL, "size for curr_placement is %u\n", temp);
    
    if(temp != 0){
        this->curr_placement = DataTransfer::deserializePlacement(in.substr(cc, temp));
        cc += temp;
    }
    else{
        this->curr_placement = nullptr;
    }

//    DPRINTF(DEBUG_UTIL, "33333\n");
    
    if(cc + sizeof(int) > in.size()){
        DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
    }
    for(uint32_t i = 0; i < sizeof(int) && cc < in.size(); i++){
        ((char*)(&(this->reconfig_state)))[i] = in[cc++];
    }
//    DPRINTF(DEBUG_UTIL, "reconfig_state is %u\n", reconfig_state);

//    DPRINTF(DEBUG_UTIL, "444444\n");
    
    if(reconfig_state != 0){

//        DPRINTF(DEBUG_UTIL, "DOOMED\n");
        
        // timestamp
        temp = 0;
        if(cc + sizeof(uint32_t) > in.size()){
            DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
        }
        for(uint32_t i = 0; i < sizeof(uint32_t) && cc < in.size(); i++){
            ((char*)(&(temp)))[i] = in[cc++];
        }
        if(temp != 0){
            for(uint32_t i = 0; i < temp && cc < in.size(); i++){
                this->timestamp.push_back(in[cc++]);
            }
        }
        else{
            this->timestamp = "";
        }
        
        
        if(cc + sizeof(uint32_t) > in.size()){
            DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
        }
        for(uint32_t i = 0; i < sizeof(uint32_t) && cc < in.size(); i++){
            ((char*)(&(this->next_conf_id)))[i] = in[cc++];
        }
        
        // next placement
        temp = 0;
        if(cc + sizeof(uint32_t) > in.size()){
            DPRINTF(DEBUG_UTIL, "BAD FORMAT INPUT\n");
        }
        for(uint32_t i = 0; i < sizeof(uint32_t) && cc < in.size(); i++){
            ((char*)(&(temp)))[i] = in[cc++];
        }
        if(temp != 0){
            this->curr_placement = DataTransfer::deserializePlacement(in.substr(cc, temp));
            cc += temp;
        }
        else{
            this->next_placement = nullptr;
        }
    }
    else{
        this->next_placement = nullptr;
    }

//    DPRINTF(DEBUG_UTIL, "55555\n");
    
}

Reconfig_key_info::~Reconfig_key_info(){
    if(curr_placement != nullptr){
        delete curr_placement;
        curr_placement = nullptr;
    }
    if(next_placement != nullptr){
        delete next_placement;
        next_placement = nullptr;
    }
}

std::string Reconfig_key_info::get_string(){
    std::string ret;
    for(uint i = 0; i < sizeof(uint32_t); i++){
        ret.push_back(((char*)(&curr_conf_id))[i]);
    }
    
    uint32_t size;
    if(this->curr_placement != nullptr){
        std::string pl_s = DataTransfer::serializePlacement(*(this->curr_placement));
        size = pl_s.size();
        for(uint32_t i = 0; i < sizeof(uint32_t); i++){
            ret.push_back(((char*)(&size))[i]);
        }
        ret += pl_s;
//        DPRINTF(DEBUG_UTIL, "curr_placement size is %u\n", size);
    }
    else{
        size = 0;
        for(uint32_t i = 0; i < sizeof(uint32_t); i++){
            ret.push_back(((char*)(&size))[i]);
        }
    }
    
    for(uint i = 0; i < sizeof(int); i++){
        ret.push_back(((char*)(&reconfig_state))[i]);
    }

//    DPRINTF(DEBUG_UTIL, "reconfig_state is %u\n", reconfig_state);
    
    if(reconfig_state != 0){
        // Timestamp
        uint32_t size = this->timestamp.size();
        for(uint32_t i = 0; i < sizeof(uint32_t); i++){
            ret.push_back(((char*)(&size))[i]);
        }
        for(uint32_t i = 0; i < size; i++){
            ret.push_back(this->timestamp[i]);
        }
        
        for(uint i = 0; i < sizeof(uint32_t); i++){
            ret.push_back(((char*)(&next_conf_id))[i]);
        }
        
        if(this->next_placement != nullptr){
            std::string pl_s = DataTransfer::serializePlacement(*(this->next_placement));
            size = pl_s.size();
            for(uint32_t i = 0; i < sizeof(uint32_t); i++){
                ret.push_back(((char*)(&size))[i]);
            }
            ret += pl_s;
        }
        else{
            size = 0;
            for(uint32_t i = 0; i < sizeof(uint32_t); i++){
                ret.push_back(((char*)(&size))[i]);
            }
        }
    }
    
    return ret;
}

Data_handler::~Data_handler(){
    if(reconfig_info != nullptr){
        delete reconfig_info;
        reconfig_info = nullptr;
    }
}

std::string construct_key(const std::string& key, const std::string& protocol, const uint32_t conf_id,
        const std::string& timestamp){
    std::string ret;
    ret += key;
    ret += "!";
    ret += protocol;
    ret += "!";
    ret += std::to_string(conf_id);
    ret += "!";
    ret += timestamp;
    return ret;
}

std::string construct_key(const std::string& key, const std::string& protocol, const uint32_t conf_id){
    std::string ret;
    ret += key;
    ret += "!";
    ret += protocol;
    ret += "!";
    ret += std::to_string(conf_id);
    return ret;
}

int request_placement(const std::string& metadata_server_ip, const std::string& metadata_server_port,
        const std::string& key, const uint32_t conf_id, std::string& status, std::string& msg, Placement*& p,
        uint32_t retry_attempts, uint32_t metadata_server_timeout){
    
    DPRINTF(DEBUG_CLIENT_NODE, "started\n");
    int ret = 0;

    DPRINTF(DEBUG_CLIENT_NODE, "metadata_server_ip port is %s %s\n", metadata_server_ip.c_str(), metadata_server_port.c_str());
    fflush(stdout);
    Connect c(metadata_server_ip, metadata_server_port);
    if(!c.is_connected()){
        DPRINTF(DEBUG_CLIENT_NODE, "connection error\n");
        return -1;
    }

//    std::string status;
    msg = key;
    msg += "!";
    msg += std::to_string(conf_id);
    
    p = nullptr;


//    DataTransfer::sendMsg(*c,DataTransfer::serializeMDS("ask", msg));
    std::string recvd;
    uint32_t RAs = retry_attempts;
    std::chrono::milliseconds span(metadata_server_timeout);
    
    bool flag = false;
    while(RAs--){
        std::promise <std::string> data_set;
        std::future <std::string> data_set_fut = data_set.get_future();
        DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("ask", msg));
        std::future<int> fut = std::async(std::launch::async, DataTransfer::recvMsg_async, *c, std::move(data_set));
        
        if(data_set_fut.valid()){
//            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is valid\n");
            std::future_status aaa = data_set_fut.wait_for(span);
            if(aaa == std::future_status::ready){
                int ret = fut.get();
//                DPRINTF(DEBUG_CLIENT_NODE, "Future ret value is %d\n", ret);
                if(ret == 1){
                    flag = true;
                    recvd = data_set_fut.get();
                    break;
                }
            }
//            DPRINTF(DEBUG_CLIENT_NODE, "aaaa is %d and to is %d\n", aaa, std::future_status::timeout);
        }
        else{
            DPRINTF(DEBUG_CLIENT_NODE, "data_set_fut is not valid\n");
        }
    }

//    c.unlock();
    
    if(flag){
        msg.clear();
        p = DataTransfer::deserializeMDS(recvd, status, msg);
    }
    else{
        ret = -2;
        DPRINTF(DEBUG_CLIENT_NODE, "Metadata server timeout for request: %s\n", msg.c_str());
    }
    
    return ret;
}

int ask_metadata(const std::string& metadata_server_ip, const std::string& metadata_server_port,
        const std::string& key, const uint32_t conf_id, uint32_t& requested_conf_id, uint32_t& new_conf_id,
        std::string& timestamp, Placement*& p, uint32_t retry_attempts, uint32_t metadata_server_timeout){
    int ret = 0;
    
    std::string status, msg;
//    Placement* p;

//    DPRINTF(DEBUG_CAS_Client, "calling request_placement....\n");
    
    ret = request_placement(metadata_server_ip, metadata_server_port, key, conf_id, status, msg, p, retry_attempts,
            metadata_server_timeout);
    
    assert(ret == 0);
    assert(status == "OK");
    
    std::size_t pos = msg.find("!");
    std::size_t pos2 = msg.find("!", pos + 1);
    if(pos >= msg.size() || pos2 >= msg.size()){
        std::stringstream pmsg; // thread safe printing
        pmsg << "BAD FORMAT: " << msg << std::endl;
        std::cerr << pmsg.str();
        assert(0);
    }
    requested_conf_id = stoul(msg.substr(0, pos));
    new_conf_id = stoul(msg.substr(pos + 1, pos2 - pos - 1));
    timestamp = msg.substr(pos2 + 1);
    
    return ret;
}

template<typename T>
void set_intersection(const Placement& p, std::unordered_set <T>& res){
    res.clear();
    res.insert(p.Q1.begin(), p.Q1.end());
    res.insert(p.Q2.begin(), p.Q2.end());
    res.insert(p.Q3.begin(), p.Q3.end());
    res.insert(p.Q4.begin(), p.Q4.end());
}

template void set_intersection(const Placement& p, std::unordered_set <unsigned int>& res);

Logger::Logger(const std::string& file_name, const std::string& function_name, const int& line_number, bool logging_on) :
                        file_name(file_name), function_name(function_name), logging_on(logging_on){
    output << "Time " << std::setw(10) << time(nullptr) << " - Thread: " << pthread_self() << " : [" << this->file_name << "]"
            << "[" << this->function_name << "]:\n";

    timer = time_point_cast<microseconds>(steady_clock::now());
    last_lapse = timer;

    output << "    " << std::setw(10) << 0 << ":" << std::setfill('0') << std::setw(4) << line_number << ": started\n" << std::setfill(' ');
}

Logger::Logger(const std::string& file_name, const std::string& function_name, const int& line_number,
               const std::string& msg, bool logging_on) : file_name(file_name), function_name(function_name), logging_on(logging_on){
    output << "Time " << std::setw(10) << time(nullptr) << " - Thread: " << pthread_self() << " : [" << this->file_name << "]"
           << "[" << this->function_name << "]:\n";

    timer = time_point_cast<microseconds>(steady_clock::now());
    last_lapse = timer;

    output << "    " << std::setw(10) << 0 << ":" << std::setfill('0') << std::setw(4) << line_number << ": started, ";
    output << msg << "\n" << std::setfill(' ');
}

Logger::~Logger(){
    time_point<steady_clock, microseconds> new_lapse = time_point_cast<microseconds>(steady_clock::now());
    output << "    " << std::setw(10) << (new_lapse - last_lapse).count() << ": finished, " << "elapsed time = " << (new_lapse - timer).count() << "micros\n";

    if(logging_on){
        std::cout << output.str() << std::flush;
    }
}

void Logger::operator()(const int& line_number){
    time_point<steady_clock, microseconds> new_lapse = time_point_cast<microseconds>(steady_clock::now());
    output << "    " << std::setw(10) << (new_lapse - last_lapse).count() << ":" << std::setfill('0') << std::setw(4) << line_number << "\n" << std::setfill(' ');
    last_lapse = new_lapse;
}

void Logger::operator()(const int& line_number, const std::string& msg){
    time_point<steady_clock, microseconds> new_lapse = time_point_cast<microseconds>(steady_clock::now());
    output << "    " << std::setw(10) << (new_lapse - last_lapse).count() << ":" << std::setfill('0') << std::setw(4) << line_number << ": " << msg << "\n" << std::setfill(' ');
    last_lapse = new_lapse;
}
