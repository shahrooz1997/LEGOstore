#include "Util.h"
#include <iostream>
#include "Data_Transfer.h"
#include <thread>
#include <algorithm>
#include <sys/wait.h>
#include <cmath>
#include "CAS_Client.h"
#include "ABD_Client.h"
#include "Client_Node.h"

using std::cout;
using std::endl;

// All clients use this for read-only access
Properties *cc = nullptr;

//Format of THread ID given to each CAS_Client:
// bit  32 is 1 for ABD and 0 for CAS
// bits 31-24 => Client ID || max : 128
// bits 12-23 => Key Id  || max : 4096
// bits 0-11 => thread num per keyID || max : 4096
int clientId;

std::atomic<bool> running(false);
std::atomic<int> numt(0);

using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;

static std::unordered_set<std::string> initialized_keys;

static inline uint32_t create_threadId(int &client_id, int &grp_id, const int &req_idx){

    std::string protocol = CAS_PROTOCOL_NAME; // Todo: remove this variable;
    
    uint32_t id = 0;
    // Checking the bounds of each index
    if((req_idx < (1 << 12)) && (grp_id < (1 << 12)) && (clientId < (1 << 7))){
            id = ((clientId << 24) | (grp_id << 12) | (req_idx));
    }else{
            throw std::logic_error("The thread ID can be redundant. Idx exceeded its bound");
    }

    if(protocol == ABD_PROTOCOL_NAME){
        id |= (1 << 31);
    }

    return id;
}

void initialise_db2(Properties *prop, GroupConfig *grpCfg, int grp_id){
    uint32_t threadId = create_threadId(clientId, grp_id, 0);
    Client_Node clt(threadId, prop->local_datacenter_id, prop->datacenters);
    
    std::vector<std::thread> pool;

    for(auto &key: grpCfg->keys){

        std::pair<std::unordered_set<std::string>::iterator,bool> ins_ret = initialized_keys.emplace(key);
        if(!ins_ret.second){
            continue;
        }

//      std::string val(grpCfg->object_size, 'a'+ (grp_id%26));
        std::string val(grpCfg->object_size, 'a');
        for(uint i = 0; i < grpCfg->object_size; i++){
            val[i] = key[i % key.size()];
        }
        pool.emplace_back(&Client_Node::put, &clt, key, val, true);
        std::cout<< "Initialisation of key : " << key << " is done!!" << std::endl;

//        numt += 1;
    }

    for(auto &it:pool){
            it.join();
    }
    
    return;
}

//void initialise_db(Properties *prop, GroupConfig *grpCfg, int grp_id){
//    
//    if(grpCfg->placement_p->protocol == CAS_PROTOCOL_NAME){
//        CAS_Client client(create_threadId(grpCfg->placement_p->protocol, clientId, grp_id, 0), prop->local_datacenter_id, prop->datacenters);
//
//    //    cout << "placement_p" << grpCfg->placement_p->Q1.size() << " " << grpCfg->placement_p->Q2.size() << " " << grpCfg->placement_p->Q3.size() << " " << grpCfg->placement_p->Q4.size() << " " << grpCfg->placement_p->protocol << endl;
//
//
//        std::vector<std::thread> pool;
//
//        for(auto &key: grpCfg->keys){
//
//            std::pair<std::unordered_set<std::string>::iterator,bool> ins_ret = initialized_keys.emplace(key);
//            if(!ins_ret.second){
//                continue;
//            }
//
//    //      std::string val(grpCfg->object_size, 'a'+ (grp_id%26));
//            std::string val(grpCfg->object_size, 'a');
//            for(uint i = 0; i < grpCfg->object_size; i++){
//                val[i] = key[i % key.size()];
//            }
//            pool.emplace_back(&CAS_Client::put, &client, key, val, true);
//            std::cout<< "Initialisation of key : " << key << " is done!!" << std::endl;
//
//            numt += 1;
//        }
//
//        for(auto &it:pool){
//                it.join();
//        }
//    }
//    else{ // ABD
//        ABD_Client client(prop, create_threadId(grpCfg->placement_p->protocol, clientId, grp_id, 0), *grpCfg->placement_p);
//
//    //    cout << "placement_p" << grpCfg->placement_p->Q1.size() << " " << grpCfg->placement_p->Q2.size() << " " << grpCfg->placement_p->Q3.size() << " " << grpCfg->placement_p->Q4.size() << " " << grpCfg->placement_p->protocol << endl;
//
//
//        std::vector<std::thread> pool;
//
//        for(auto &key: grpCfg->keys){
//
//            std::pair<std::unordered_set<std::string>::iterator,bool> ins_ret = initialized_keys.emplace(key);
//            if(!ins_ret.second){
//                continue;
//            }
//
//    //      std::string val(grpCfg->object_size, 'a'+ (grp_id%26));
//            std::string val(grpCfg->object_size, 'a');
//            pool.emplace_back(&ABD_Client::put, &client, key, val, true);
//            std::cout<< "Initialisation of key : " << key << " is done!!" << std::endl;
//
//            numt += 1;
//        }
//
//        for(auto &it:pool){
//                it.join();
//        }
//    }
//}

static inline int next_event(std::string &dist_process){
	if(dist_process == "uniform"){
		return 1000;
	}else if(dist_process == "poisson"){
		//TODO:: Check this!
		return -log(1 - (double)rand()/(RAND_MAX)) * 1000;
	}else{
		throw std::logic_error("Distribution process specified is unknown !! ");
	}
	return 0;
}

int run_session2(uint32_t obj_size, double read_ratio, uint64_t duration, std::vector<std::string> &keys, std::string &dist_process, int grp_id, int req_idx){
    int cnt = 0; 		// Count the number of requests
    int reqType = 0;    // 1 for GET, 2 for PUT
    int key_idx = -1;
    std::string read_value;
//    uint32_t threadId = create_threadId(pp->protocol, clientId, grp_id, req_idx);
    uint32_t threadId = create_threadId(clientId, grp_id, req_idx);
    
//    int desc = create_liberasure_instance(grpCfg->placement_p);
    
    Client_Node clt(threadId, cc->local_datacenter_id, cc->datacenters);
    srand(threadId);
    
    auto timePoint = time_point_cast<milliseconds>(system_clock::now());
    timePoint += millis{duration* 1000};
//    std::this_thread::sleep_until(timePoint);
    
    time_point<system_clock, millis> tp = time_point_cast<milliseconds>(system_clock::now());
    
    while(system_clock::now() < timePoint){
        cnt += 1;
        double random_ratio = (double)(rand() % 100) / 100.00;
        reqType = random_ratio <= read_ratio? 1:2;
        int result = S_OK;
        
        //Choose a random key
        key_idx = rand()%(keys.size());
        
        if(reqType == 1){
            result = clt.get(keys[key_idx],read_value);
            DPRINTF(DEBUG_CAS_Client, "get done on key: %s with value: %s\n", keys[key_idx].c_str(), read_value.c_str());
            fflush(stdout);
        }else{
            std::string val = std::to_string(cnt);
            for(uint i = 0; i < obj_size - std::to_string(cnt).size(); i++){
                val.push_back(keys[key_idx][i % keys[key_idx].size()]);
            }
            result = clt.put(keys[key_idx], val, false);
            DPRINTF(DEBUG_CAS_Client, "put done on key: %s with value: %s\n", keys[key_idx].c_str(), val.c_str());
            fflush(stdout);
        }
        
        assert(result != S_FAIL);

        if(result == S_RECFG)
            return 1;
        
        tp += millis{next_event(dist_process)};
        std::this_thread::sleep_until(tp);
    }
    
//    destroy_liberasure_instance(desc);
    
    return cnt;
}


//int run_session(uint32_t obj_size, double read_ratio, std::vector<std::string> &keys, Placement *pp, time_point<system_clock, millis> tp,
//							std::string &dist_process, int grp_id, int req_idx, int desc){
//    int cnt = 0; 		// Count the number of requests
//    int reqType = 0;    // 1 for GET, 2 for PUT
//    int key_idx = -1;
//    uint32_t threadId = create_threadId(pp->protocol, clientId, grp_id, req_idx);
//
//    if(pp->protocol == CAS_PROTOCOL_NAME){
//        CAS_Client clt(threadId, cc->local_datacenter_id, cc->datacenters, desc);
//        //Scope of this seeding is global
//        //So, may lead to same operation sequence on all threads
//        srand(threadId);
//
//        std::string read_value;
//        while(running.load()){
//            cnt += 1;
//            // Choose the operation type
//            double random_ratio = (double)(rand() % 100) / 100.00;
//            reqType = random_ratio <= read_ratio? 1:2;
//            int result = S_OK;
//
//            //Choose a random key
//            key_idx = rand()%(keys.size());
//    //	std::cout << "Operation chosen by Client  "<<threadId<<" for key " << keys[key_idx] << "  is " << reqType << std::endl;
//            // Initiate the operation
//            if(reqType == 1){
//                result = clt.get(keys[key_idx],read_value);
//                DPRINTF(DEBUG_CAS_Client, "get done on key: %s with value: %s\n", keys[key_idx].c_str(), read_value.c_str());
////                cout << "get done on key: " << keys[key_idx] << " with value " << read_value << endl;
//                fflush(stdout);
//            }else{
//                std::string val(obj_size, 'a');
//                result = clt.put(keys[key_idx], val, false);
//                DPRINTF(DEBUG_CAS_Client, "put done on key: %s with value: %s\n", keys[key_idx].c_str(), val.c_str());
////                cout << "put done on key: " << keys[key_idx] << endl;
//                fflush(stdout);
//            }
//
//            assert(result != S_FAIL);
//
//            if(result == S_RECFG)
//                return 1;
//
//            tp += millis{next_event(dist_process)};
//            std::this_thread::sleep_until(tp);
//        }
//
//        //std::cout<<"COunt value before returning" << cnt << std::endl;
//        numt += cnt;
//    }
//    else{ // ABD
//        ABD_Client clt(cc, threadId, *pp);
//        //Scope of this seeding is global
//        //So, may lead to same operation sequence on all threads
//        srand(threadId);
//
//        std::string read_value;
//        while(running.load()){
//            cnt += 1;
//            // Choose the operation type
//            double random_ratio = (double)(rand() % 100) / 100.00;
//            reqType = random_ratio <= read_ratio? 1:2;
//            int result = S_OK;
//
//            //Choose a random key
//            key_idx = rand()%(keys.size());
//    //	std::cout << "Operation chosen by Client  "<<threadId<<" for key " << keys[key_idx] << "  is " << reqType << std::endl;
//            // Initiate the operation
//            if(reqType == 1){
//                result = clt.get(keys[key_idx],read_value);
//                cout << "get done on key: " << keys[key_idx] << "with value " << read_value << endl;
//            }else{
//                std::string val(obj_size, 'a');
//                result = clt.put(keys[key_idx], val, false);
//                cout << "put done on key: " << keys[key_idx] << endl;
//            }
//
//            assert(result != S_FAIL);
//
//            if(result == S_RECFG)
//                return 1;
//
//            tp += millis{next_event(dist_process)};
//            std::this_thread::sleep_until(tp);
//        }
//
//        //std::cout<<"COunt value before returning" << cnt << std::endl;
//        numt += cnt;
//    }
//
//    return 0;
//}


//NOTE:: A process is started for each grp_id
//NOTE:: Grp_id is just a unique identifier for this grp, can be modified easily later on

int key_req_gen(int grp_idx, int grp_config_idx, int grp_id, std::string dist_process){

    Properties &prop = *cc;
    clientId = prop.local_datacenter_id;
    int avg = 0;

    //int act_cnt = 0;
    GroupConfig *grpCfg = prop.groups[grp_idx]->grp_config[grp_config_idx];
//    auto timePoint = time_point_cast<milliseconds>(system_clock::now());

    // After this call all keys will be initialized
    // This assumes keys and key groups are not added while experiment happening
    initialise_db2(cc, grpCfg, grp_id);

//    int desc = -1;
    
//    if(grpCfg->placement_p->protocol == CAS_PROTOCOL_NAME){
//        desc = create_liberasure_instance(grpCfg->placement_p);
//    }
    
    printf("Creating init threads for grp id: %d \n", grp_idx);

//    running = true;
    //Start threads
//    std::vector<std::thread> threads;

    //Round up to nearest integer value
    long numReqs = lround(grpCfg->client_dist[clientId] * grpCfg->arrival_rate);


    for(long i=0; i<numReqs; i++){
        fflush(stdout);
        if(fork() == 0){
            int cnt = run_session2(grpCfg->object_size, grpCfg->read_ratio, grpCfg->duration, grpCfg->keys, dist_process, grp_id, i);
            exit(cnt);
        }
//            threads.emplace_back(run_session, grpCfg->object_size, grpCfg->read_ratio, std::ref(grpCfg->keys), grpCfg->placement_p, timePoint, std::ref(dist_process), grp_id, i, desc);
    }

//    timePoint += millis{grpCfg->duration* 1000};
//    std::this_thread::sleep_until(timePoint);

//    running = false;
//    for(auto &th: threads){
//            th.join();
//    }
    
    int total = 0;
//    float avg_rate = 0;
    int ret_val =0;
    while(wait(&ret_val) >=0){
        std::cout << "Child temination status " << WIFEXITED(ret_val) << "  Rate receved is " <<  WEXITSTATUS(ret_val) <<
                    " : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) <<std::endl;

        // TODO:: Modify this - right now accumulates rate for all processes, spread across the length of the experiment
//        avg_rate += WEXITSTATUS(ret_val);
        total += WEXITSTATUS(ret_val);
    }
    
//    if(grpCfg->placement_p->protocol == CAS_PROTOCOL_NAME){
//        destroy_liberasure_instance(desc);
//    }
    
//    std::cout<<"NUm value before returning" << numt.load() << std::endl;
    avg = total/(grpCfg->duration);

    std::cout<<"Average arrival Rate : " << avg << std::endl;

    if(avg < numReqs){
            fprintf(stderr, "WARNING!! The average arrival rate for group :%d and group_config :%d is\
                                                    less than required : %d / %ld", grp_idx, grp_config_idx, avg, numReqs);
    }
    return avg;
}

int main(int argc, char* argv[]){

    if(argc != 2){
            std::cout<<"Incorrect number of arguments: Please specify the port number" << std::endl;
            return 1;
    }
    int newSocket = socket_setup(argv[1]);
    std::cout<< "Waiting for connection from Controller." <<std::endl;
    //std::cout<< "Atomic variable is lock free? " << running.is_lock_free() << " : another opinion : " << ATOMIC_BOOL_LOCK_FREE << std::endl;
    int sock = accept(newSocket, NULL, 0);
    close(newSocket);
    
    // ToDo: there is a bug in the number of receiving messages from controller; for now it is resolved with the following variable
//    int number_of_messages = 2;
    
    std::vector<std::thread> thread_pool;

    std::string recv_str;
    try{
//        while(number_of_messages--){ // Try to receive new workloads group from Controller
//        int sock = accept(newSocket, NULL, 0);
        if(DataTransfer::recvMsg(sock, recv_str) != 1){
            std::cout<< "Client Config could not be received! " << std::endl;
            return 1;
        }
        close(sock);

        if(cc != nullptr){
            delete cc;
            cc = nullptr;
        }
        cc = DataTransfer::deserializePrp(recv_str); // Note: the received property must contain only one group

//        DPRINTF(DEBUG_CAS_Client, "new placement k attr is %u\n", cc->groups[0]->grp_config[0]->placement_p->k)

        time_point<system_clock, millis> startPoint(millis{cc->start_time});
        time_point<system_clock, millis> timePoint;
        
        // initialization
//            cout << "cc->groups[0]->grp_id.size() : " << cc->groups[0]->grp_id.size() << endl;
//        for(uint i = 0; i < cc->groups[0]->grp_id.size(); i++){
////                cout << "initialise_db called" << endl;
//            initialise_db(cc, cc->groups[0]->grp_config[i], cc->groups[0]->grp_id[i]);
//        }

        int idx = 0;
        for(auto grp: cc->groups){ // Note: we will remove this for loop in the future.
//        auto grp = cc->groups[0];
            timePoint = startPoint + millis{grp->timestamp * 1000};
            std::this_thread::sleep_until(timePoint);

            //TODO:: decide where to specify the distribiution
            // NOTE:: grp_id size and grp_config size should be equal for Groupxxxx
            for(uint i=0; i<grp->grp_id.size(); i++){
//                fflush(stdout);
                thread_pool.emplace_back(key_req_gen, idx, i, grp->grp_id[i], "poisson");
//                if(fork() == 0){
//                    cout << "key_req_gen is called for group " << grp->grp_id[i] << endl;
//                    // Note:: Grp_id can be anything, here it is provided by the config file
//                    int rate = key_req_gen(*cc, idx, i, grp->grp_id[i], "poisson");
//                    std::cout << "Rate sent is " << rate  << std::endl;
//                    exit(rate);
//                }
            }
            idx++;
        }

//        float avg_rate = 0;
//        int ret_val =0;
//        while(wait(&ret_val) >=0){
//            std::cout << "Child temination status " << WIFEXITED(ret_val) << "  Rate receved is " <<  WEXITSTATUS(ret_val) <<
//                        " : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) <<std::endl;
//
//            // TODO:: Modify this - right now accumulates rate for all processes, spread across the length of the experiment
//            avg_rate += WEXITSTATUS(ret_val);
//        }
//
//        std::cout<< "Final avg rate is " << avg_rate << std::endl;
//        }
        
        for(auto &it:thread_pool){
            it.join();
        }
    }
    catch(std::logic_error &e){
        std::cout<< "Exception caught! " << e.what() << std::endl;
    }

    //TODO:: wait for time outs before terminating the thread, cleanup purposes
    close(newSocket);
    delete cc;
    return 0;

}
