#include "Controller.h"
#include "../inc/Controller.h"
#include <typeinfo>
#include <ratio>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

using std::cout;
using std::string;
using std::vector;
using std::to_string;
using std::endl;
using std::unique_ptr;
using namespace std::chrono;
using json = nlohmann::json;

#define TOTAL_NUMBER_OF_DCS_IN_SYSTEM 9

Controller::Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t timeout_per_req, const string& detacenters_info_file,
                       const string& workload_file, const string &placements_file){
    properties.retry_attempts = retry;
    properties.metadata_server_timeout = metadata_timeout;
    properties.timeout_per_request = timeout_per_req;
    properties.local_datacenter_id = 0;

    assert(read_detacenters_info(detacenters_info_file) == 0);
    assert(read_input_workload(workload_file) == 0);
    assert(read_placements(placements_file) == 0);

    reconfigurer_p = unique_ptr<Reconfig>(new Reconfig(0, properties.local_datacenter_id, retry, metadata_timeout, timeout_per_req, properties.datacenters));

    init_metadata_server();
}

int Controller::read_detacenters_info(const string& file){
    ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
    }

    for(auto& it : j.items()){
        DC* dc = new DC;
        dc->id = stoui(it.key());
        it.value()["metadata_server"]["host"].get_to(dc->metadata_server_ip);
        dc->metadata_server_port = stoui(it.value()["metadata_server"]["port"].get<string>());

        for(auto& server : it.value()["servers"].items()){
            Server* sv = new Server;
            sv->id = stoui(server.key());
            server.value()["host"].get_to(sv->ip);

            sv->port = stoui(server.value()["port"].get<string>());
            sv->datacenter = dc;
            dc->servers.push_back(sv);
        }

        for(auto& client : it.value()["clients"].items()){
            string temp;
            client.value()["host"].get_to(temp);
            properties.clients.push_back(temp);
        }

        this->properties.datacenters.push_back(dc);
    }
    cfg.close();
    return 0;
}

int Controller::read_input_workload(const string& file){
    ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
    }

    j = j["workload_config"];
    for(auto& element: j){
        this->properties.group_configs.emplace_back();
        auto& grpcon = this->properties.group_configs.back();
        element["timestamp"].get_to(grpcon.timestamp);
        element["id"].get_to(grpcon.id);

//        vector<uint32_t> grp_ids;
//        element["grp_id"].get_to(grp_ids);

//        int counter = 0;
        for(auto& it: element["grp_workload"]){
            grpcon.groups.emplace_back();
            auto& grp = grpcon.groups.back();

            it["id"].get_to(grp.id);

//            assert(counter < grp_ids.size());
//            grp.id = grp_ids[counter++]; // ID will be set from the output of the optimizer Todo: is it OK?
            it["availability_target"].get_to(grp.availability_target);
            it["client_dist"].get_to(grp.client_dist);
            it["object_size"].get_to(grp.object_size);
//            it["metadata_size"].get_to(gwkl->metadata_size);
            it["num_objects"].get_to(grp.num_objects);
            it["arrival_rate"].get_to(grp.arrival_rate);
            it["read_ratio"].get_to(grp.read_ratio);
//            it["write_ratio"].get_to(gwkl->write_ratio);
//            it["SLO_read"].get_to(gwkl->slo_read);
//            it["SLO_write"].get_to(gwkl->slo_write);
            uint16_t dur_temp;
            it["duration"].get_to(dur_temp);
            grp.duration = seconds(dur_temp);
//            it["time_to_decode"].get_to(gwkl->time_to_decode);
            it["keys"].get_to(grp.keys);
//            (wkl->grp).push_back(gwkl);
        }
//        assert(wkl->grp_id.size() == wkl->grp.size());
//        assert(wkl->id > 0);
//        input.push_back(wkl);
    }
    cfg.close();
    return 0;
}

int Controller::read_placement_one_config(const string& file, uint32_t confid){

    string file_name = file;
    file_name.insert(file.find(".json"), string("_") + to_string(confid));

    ifstream cfg(file_name);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file_name.c_str());
        return -1;
    }

    //Find group conf index
    uint groupconf_indx = 0;
    bool groupconf_indx_found = false;
    for(; groupconf_indx < properties.group_configs.size(); groupconf_indx++){
        if(properties.group_configs[groupconf_indx].id == confid){
            groupconf_indx_found = true;
            break;
        }
    }
    if(!groupconf_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong groupconf_id %d.\n", confid);
    }
    assert(groupconf_indx_found);

    j = j["output"];
    uint32_t counter = 0;
    for(auto& element: j){
        assert(counter < this->properties.group_configs[groupconf_indx].groups.size());
        auto& grp = this->properties.group_configs[groupconf_indx].groups[counter++];

//        element["id"].get_to(grp.id);
        element["protocol"].get_to(grp.placement.protocol);
        if(grp.placement.protocol == "replication" || grp.placement.protocol == "cas"){
            grp.placement.protocol = CAS_PROTOCOL_NAME;
        }
        else{
            grp.placement.protocol = ABD_PROTOCOL_NAME;
        }
//        element["m"].get_to(grp.placement.m); // m should show the number of datacenters instead of the number of servers in the current placement
        grp.placement.m = TOTAL_NUMBER_OF_DCS_IN_SYSTEM;
        if(grp.placement.protocol != ABD_PROTOCOL_NAME){
            element["k"].get_to(grp.placement.k);
        }
        element["selected_dcs"].get_to(grp.placement.servers); // The number of servers here serves the role of m

        json client_placement_info = element["client_placement_info"];
        grp.placement.quorums.resize(TOTAL_NUMBER_OF_DCS_IN_SYSTEM);
        for(auto& it : client_placement_info.items()){
            uint32_t datacenter_id = stoui(it.key());
            it.value()["Q1"].get_to(grp.placement.quorums[datacenter_id].Q1);
            it.value()["Q2"].get_to(grp.placement.quorums[datacenter_id].Q2);
            if(grp.placement.protocol != ABD_PROTOCOL_NAME){
                it.value()["Q3"].get_to(grp.placement.quorums[datacenter_id].Q3);
                it.value()["Q4"].get_to(grp.placement.quorums[datacenter_id].Q4);
            }
        }

    }
    cfg.close();

    // Make sure all the key groups are set
    for(auto& g: properties.group_configs[groupconf_indx].groups){
        assert(g.placement.m != 0);
    }

    return 0;
}

int Controller::read_placements(const string& file){
    for(auto& gc: properties.group_configs){
        assert(this->read_placement_one_config(file, gc.id) == 0);
    }
    
    return 0;
}

int execute(const char* command, const vector<string>& args, string& output){
    
    cout << "Executing \"" << command << " ";
    for(uint i = 1; i < args.size(); i++){
        cout << args[i];
        if(i != args.size() - 1)
            cout << " ";
    }
    cout << "\"...\n";
    
    
    int pipedes[2];
    if(pipe(pipedes) == -1) {
        perror("pipe");
        return -1;
    }
    
    pid_t pid = fork();
    if (pid == -1){
        perror("fork");
        return -2;
    }
    else if(pid == 0){ // Child process
        while ((dup2(pipedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {} // we used while so it will happen even if an interrupt arises
        while ((dup2(pipedes[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
        close(pipedes[1]);
        close(pipedes[0]);
        switch(args.size()){
            case 1:
                execl(command, args[0].c_str(), (char *) 0);
                break;
            
            case 2:
                execl(command, args[0].c_str(), args[1].c_str(), (char *) 0);
                break;
            
            case 3:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), (char *) 0);
                break;
            
            case 4:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), (char *) 0);
                break;
            
            case 5:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), (char *) 0);
                break;
            
            case 6:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), (char *) 0);
                break;
            
            case 7:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), (char *) 0);
                break;

            case 8:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), (char *) 0);
                break;

            case 9:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), (char *) 0);
                break;

            case 10:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), (char *) 0);
                break;

            case 11:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), (char *) 0);
                break;

            case 12:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), (char *) 0);
                break;

            case 13:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), args[12].c_str(), (char *) 0);
                break;

            case 14:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), args[12].c_str(), args[13].c_str(), (char *) 0);
                break;
        }
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
        perror("execl");
        _exit(1);
    }
    else {
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
        close(pipedes[1]);
        int ret_val;
        wait(&ret_val);
        
        ret_val = WEXITSTATUS(ret_val);
        
        char buffer[4096];
        while(true){
            ssize_t count = read(pipedes[0], buffer, sizeof(buffer));
            if (count == -1) {
                if (errno == EINTR) {
                    continue;
                }
                else{
                    perror("read");
                    return -3;
                }
            }
            else if(count == 0){
                break;
            }
            else{
                output.append(buffer, count);
            }
        }
        close(pipedes[0]);
        return ret_val;
    }
    
}

//"<datacenter_id> <retry_attempts_number> "
//"<metadata_server_timeout> <timeout_per_request> <conf_id> "
//"<grp_id> <object_size> <arrival_rate> <read_ratio>"
//"<duration> <num_objects>"
int Controller::run_client(uint32_t datacenter_id, uint32_t conf_id, uint32_t group_id){

#ifdef LOCAL_TEST
    vector<string> args;
    string output;
    string command = "./Client";

    args.push_back(command);
    args.push_back(to_string(datacenter_id));
    args.push_back(to_string(properties.retry_attempts));
    args.push_back(to_string(properties.metadata_server_timeout));
    args.push_back(to_string(properties.timeout_per_request));
    args.push_back(to_string(conf_id)); // Todo: Confid must be determined by the workload file
    args.push_back(to_string(group_id));

    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < properties.group_configs.size(); configuration_indx++){
        if(properties.group_configs[configuration_indx].id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint group_indx = 0;
    bool config_group_found = false;
    for(; group_indx < properties.group_configs[configuration_indx].groups.size(); group_indx++){
        if(properties.group_configs[configuration_indx].groups[group_indx].id == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < properties.datacenters.size(); datacenter_indx++){
        if(properties.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

    const Group& gc = properties.group_configs[configuration_indx].groups[group_indx];
    args.push_back(to_string(gc.object_size));
    args.push_back(to_string(gc.arrival_rate * gc.client_dist[datacenter_indx]));
    args.push_back(to_string(gc.read_ratio));
    args.push_back(to_string(duration_cast<seconds>(gc.duration).count()));
    args.push_back(to_string(gc.keys.size()));

    int status = execute(command.c_str(), args, output);

    if(status == 0){
        cout << output << endl;
        return 0;
    }
    else{
        if(output != ""){
            if(output.find("ERROR") != string::npos){
                cerr << "error in execution " << command << endl;
                cerr << output << endl;
                return -4;
            }
        }
        else{
            cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
            return -4;
        }
    }

    return 0;
#else
    string command = "cd project; ./Client ";

    command += to_string(datacenter_id) + " ";
    command += to_string(properties.retry_attempts)+ " ";
    command += to_string(properties.metadata_server_timeout)+ " ";
    command += to_string(properties.timeout_per_request)+ " ";
    command += to_string(conf_id)+ " "; // Todo: Confid must be determined by the workload file
    command += to_string(group_id)+ " ";

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "11111111111111\n");
    
    // finding configuration index based on the configuration id
    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < properties.group_configs.size(); configuration_indx++){
        if(properties.group_configs[configuration_indx].id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint group_indx = 0;
    bool config_group_found = false;
    for(; group_indx < properties.group_configs[configuration_indx].groups.size(); group_indx++){
        if(properties.group_configs[configuration_indx].groups[group_indx].id == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < properties.datacenters.size(); datacenter_indx++){
        if(properties.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "222222222222\n");
    const Group& gc = properties.group_configs[configuration_indx].groups[group_indx];
    command += to_string(gc.object_size)+ " ";
    command += to_string(gc.arrival_rate * gc.client_dist[datacenter_indx])+ " ";
    command += to_string(gc.read_ratio)+ " ";
    command += to_string(duration_cast<seconds>(gc.duration).count()) + " ";
    command += to_string(gc.keys.size());
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "333333333333\n");


    vector<string> args = {"ssh", "-o", "StrictHostKeyChecking no", "-t", properties.clients[datacenter_indx], command};
    string output;
    int status = execute("/usr/bin/ssh", args, output);
    
    if(status == 0){
        cout << output << endl;
        return 0;
    }
    else{
        if(output != ""){
            if(output.find("ERROR") != string::npos){
                cerr << "error in execution " << command << endl;
                cerr << output << endl;
                return -4;
            }
        }
        else{
            cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
            return -4;
        }
    }
    
    return 0;

#endif
}

int Controller::init_metadata_server(){
    assert(properties.group_configs.size() > 0);
    vector<future<int>> rets;

    const Group_config& gc = this->properties.group_configs[0];
    for(uint i = 0; i < gc.groups.size(); i++){
        for(uint j = 0; j < gc.groups[i].keys.size(); j++){
            string key = gc.groups[i].keys[j];
            uint32_t conf_id = gc.id;
            rets.emplace_back(async(launch::async, &Reconfig::update_metadata_info, this->reconfigurer_p.get(), key,
                                    conf_id, conf_id, "", gc.groups[i].placement));
        }
    }

    for(auto it = rets.begin(); it != rets.end(); it++){
        if(it->get() != S_OK){
            assert(false);
        }
    }
    return 0;
}

// Return true if match
static inline bool compare_placement(const Placement& old, const Placement& curr){
    if(old.protocol != curr.protocol){
        return false;
    }
    if(old.protocol == ABD_PROTOCOL_NAME){
        for(uint i = 0; i < old.quorums.size() && i < curr.quorums.size(); i++){
            if(old.quorums[i].Q1 != curr.quorums[i].Q1 || old.quorums[i].Q2 != curr.quorums[i].Q2){
                return false;
            }
        }
        return true;
    }
    else{
        for(uint i = 0; i < old.quorums.size() && i < curr.quorums.size(); i++){
            if(old.quorums[i].Q1 != curr.quorums[i].Q1 || old.quorums[i].Q2 != curr.quorums[i].Q2){
                return false;
            }
            if(old.quorums[i].Q3 != curr.quorums[i].Q3 || old.quorums[i].Q4 != curr.quorums[i].Q4){
                return false;
            }
            if(old.k != curr.k){
                return false;
            }
        }
        return true;
    }
    return true;
}

int Controller::run_clients_for(int group_config_i){ //Todo: run other clients as well.
    for(uint i = 0; i < properties.group_configs[group_config_i].groups.size(); i++){
        for(uint j = 0; j < properties.datacenters.size(); j++){
//            DPRINTF(DEBUG_RECONFIG_CONTROL, "aaaa %s\n", master.prp.datacenters[j]->metadata_server_ip.c_str());
            if(properties.group_configs[group_config_i].groups[i].client_dist[j] != 0){
                clients_thread.emplace_back(&Controller::run_client, this, properties.datacenters[j]->id, properties.group_configs[group_config_i].id,
                                            properties.group_configs[group_config_i].groups[i].id);
            }
        }
    }
    return S_OK;
}

int Controller::run_all_clients(){ //Todo: run other clients as well.
    auto startPoint = time_point_cast<milliseconds>(system_clock::now());
    time_point <system_clock, milliseconds> timePoint;
    for(uint i = 0; i < properties.group_configs.size(); i++){
        auto& gc = properties.group_configs[i];
        timePoint = startPoint + milliseconds{gc.timestamp * 1000};
        this_thread::sleep_until(timePoint);
        DPRINTF(DEBUG_CAS_Client, "clients in group_configs %d started.\n", i);
        this->run_clients_for(i);
    }
    return S_OK;
}

Group& find_old_configuration(Properties& prp, uint curr_group_id, uint curr_conf_id, uint& old_conf_id){

    uint conf_id_indx = 0;
    bool conf_id_indx_found = false;
    for(; conf_id_indx < prp.group_configs.size(); conf_id_indx++){
        if(prp.group_configs[conf_id_indx].id == curr_conf_id){
            conf_id_indx_found = true;
            break;
        }
    }
    if(!conf_id_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong curr_conf_id %d.\n", curr_conf_id);
    }
    assert(conf_id_indx_found);

    for(uint i = conf_id_indx - 1; i >= 0; i--){
        for(uint j = 0; j < prp.group_configs[i].groups.size(); j++){
            if(prp.group_configs[i].groups[j].id == curr_group_id){
                old_conf_id = prp.group_configs[i].id;
                return prp.group_configs[i].groups[j];
            }
        }
    }

    assert(false);
//    return nullptr;
}

int Controller::run_reconfigurer(){ //Todo: make it more reliable
    auto startPoint = time_point_cast<milliseconds>(system_clock::now());
    time_point <system_clock, milliseconds> timePoint;
    for(uint i = 1; i < properties.group_configs.size(); i++){
        auto& gc = properties.group_configs[i];
        timePoint = startPoint + milliseconds{gc.timestamp * 1000};
        this_thread::sleep_until(timePoint);

        for(uint j = 0; j < gc.groups.size(); j++){
            Group& curr = gc.groups[j];
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Starting the reconfiguration for group id %u\n", curr.id);

            //Todo: WTH!!!! Put config group id in its own struct
            uint old_conf_id;
            Group& old = find_old_configuration(properties, curr.id, gc.id, old_conf_id);

            auto epoch = time_point_cast<chrono::microseconds>(chrono::system_clock::now()).time_since_epoch().count();
            reconfigurer_p->reconfig(old, old_conf_id, curr, gc.id);
            auto epoch2 = time_point_cast<chrono::microseconds>(chrono::system_clock::now()).time_since_epoch().count();
            cout << "reconfiguration latency: " << (double)(epoch2 - epoch) / 1000000. << endl;

        }

    }
    return S_OK;
}

int Controller::wait_for_clients(){
    for(auto it = clients_thread.begin(); it != clients_thread.end(); it++){
        it->join();
    }
    return S_OK;
}

int warm_up_one_connection(const string& ip, uint32_t port){

    string temp = string(WARM_UP_MNEMONIC) + get_random_string();
    Connect c(ip, port);
    if(!c.is_connected()){
        assert(false);
        return -1;
    }
    DataTransfer::sendMsg(*c, temp);
    string recvd;
    if(DataTransfer::recvMsg(*c, recvd) == 1){
        if(!is_warmup_message(recvd)){
            assert(false);
        }
    }
    else{
        assert(false);
    }

    return S_OK;
}

int Controller::warm_up(){

    for(auto it = properties.datacenters.begin(); it != properties.datacenters.end(); it++){
        warm_up_one_connection((*it)->metadata_server_ip, (*it)->metadata_server_port);
        this_thread::sleep_for(milliseconds(get_random_number_uniform(0, 2000)));
    }

    for(auto it = properties.datacenters.begin(); it != properties.datacenters.end(); it++){
        warm_up_one_connection((*it)->servers[0]->ip, (*it)->servers[0]->port);
        this_thread::sleep_for(milliseconds(get_random_number_uniform(0, 2000)));
    }

    return S_OK;
}

// Controller has three main responsibilities: one is to communicate with metadata servers, two is to do reconfiguration,
// and three is run clients for testing.
int main(){

#ifdef LOCAL_TEST
    #ifdef GCS
    // Below timeout's for GCS could be improved on a dedicated system. 
    uint32_t metadata_timeout_per_request = 30000;
    uint32_t timeout_per_request = 30000;
    #else
    uint32_t metadata_timeout_per_request = 10000;
    uint32_t timeout_per_request = 10000;
    #endif
    Controller master(2, metadata_timeout_per_request, timeout_per_request, "./config/local_config.json",
                      "./config/auto_test/input_workload.json", "./config/auto_test/optimizer_output.json");
#else
    Controller master(2, 10000, 10000, "./config/auto_test/datacenters_access_info.json",
                      "./config/auto_test/input_workload.json", "./config/auto_test/optimizer_output.json");
#endif
    std::future<int> client_executer_fut = std::async(&Controller::run_all_clients, &master);
//    master.run_all_clients();
    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");

#ifdef DO_WARM_UP
    auto warm_up_tp = time_point_cast<milliseconds>(system_clock::now());;
    warm_up_tp += seconds(WARM_UP_DELAY);
    master.warm_up();
    this_thread::sleep_until(warm_up_tp);
#endif

    DPRINTF(DEBUG_RECONFIG_CONTROL, "run_reconfigurer\n");
    master.run_reconfigurer();

    client_executer_fut.get();
    master.wait_for_clients();

    DPRINTF(DEBUG_RECONFIG_CONTROL, "Connect::close_all()\n");
    Connect::close_all();
    return 0;
}
