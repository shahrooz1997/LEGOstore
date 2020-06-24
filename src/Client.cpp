#include "Util.h"
#include <iostream>
#include "Data_Transfer.h"
#include <thread>
#include <algorithm>
#include <sys/wait.h>
#include <cmath>
#include "CAS_Client.h"

//TODO:: Remove this, jyst for experiment purposes
Properties test;

//Format of THread ID given to each CAS_Client:
// bits 32-24 => Client ID || max : 256
// bits 12-23 => Key Id  || max : 4096
// bits 0-11 => thread num per keyID || max : 4096
int clientId;

std::atomic<bool> running(false);
std::atomic<int> numt(0);

using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;

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

static inline uint32_t create_threadId(int &client_id, int &grp_idx, const int &req_idx){

	uint32_t id = 0;
	// Checking the bounds of each index
	if( req_idx < (1<<12) && grp_idx < (1<<12) && clientId < (1<<8) ){
		id = ((clientId << 24) | (grp_idx << 12) | (req_idx));
	}else{
		throw std::logic_error("The thread ID can be redundant. Idx exceeded its bound");
	}

	return id;
}
int run_session(uint32_t obj_size, double read_ratio, std::vector<std::string> &keys, Placement *pp, time_point<system_clock, millis> tp,
							std::string &dist_process, int grp_idx, int req_idx, int desc){
	int cnt = 0; 		// Count the number of requests
	int reqType = 0;    // 1 for GET, 2 for PUT
	int key_idx = -1;
	uint32_t threadId = create_threadId(clientId, grp_idx, req_idx);
	CAS_Client clt(test, threadId,*pp, desc);
	//Scope of this seeding is global
	//So, may lead to same operation sequence on all threads
	srand(threadId);

	std::string read_value;
	while(running.load()){
		cnt+= 1;
		// Choose the operation type
		double random_ratio = (double)(rand() % 100) / 100.00;
		reqType = random_ratio <= read_ratio? 1:2;

		//Choose a random key
		key_idx = rand()%(keys.size());
	//	std::cout << "Operation chosen by Client  "<<threadId<<" for key " << keys[key_idx] << "  is " << reqType << std::endl;
		// Initiate the operation
		if(reqType == 1){
			clt.get(keys[key_idx],read_value);
		}else{
			std::string val(obj_size, 'a');
			clt.put(keys[key_idx], val, false);
		}

		tp += millis{next_event(dist_process)};
		std::this_thread::sleep_until(tp);
	}

	std::cout<<"COunt value before returning" << cnt << std::endl;
	numt += cnt;

	return 0;
}
//NOTE:: A process is started for each grp_id
//NOTE:: Grp key idx should match with its index in grp_config
int key_req_gen(Properties &prop, int grp_idx, std::string dist_process){

	clientId = prop.local_datacenter_id;
	uint64_t startTime = prop.start_time;
	time_point<system_clock, millis> timePoint(millis{startTime});

	std::this_thread::sleep_until(timePoint);
	int avg = 0;
	bool key_init = true;	// True only for the first iteration of each grp
				// After the first iteration all keys will be initialized
				// This assumes keys are not added while experiment happening
	int act_cnt = 0;
	GroupConfig *grpCfg;

	for(uint j=0; j<prop.groups.size() ; j++){
		// If the key id doesn't exist in this timestamp, skip it
		auto it = find(prop.groups[j]->grp_id.begin(), prop.groups[j]->grp_id.end(), grp_idx);
		if( it == prop.groups[j]->grp_id.end()){
			++j;
			continue;
		}

		act_cnt++;
		// The config for this grp index
		grpCfg = prop.groups[j]->grp_config[it - prop.groups[j]->grp_id.begin()];

		int desc = create_liberasure_instance(grpCfg->placement_p);

		// Prep the database before the next epoch, write objects of specified size
		printf("Creating init threads for grp id: %d \n", grp_idx);
		CAS_Client client(test, create_threadId(clientId, grp_idx, 0),  *grpCfg->placement_p, desc);
		for(auto &key: grpCfg->keys){
			std::string val(grpCfg->object_size, 'a'+ (grp_idx%26));
			client.put(key, val, key_init);
			numt += 1;
		}

		running = true;
		//Start threads
		std::vector<std::thread> threads;

		//Round up to nearest integer value
		long numReqs = lround(grpCfg->client_dist[clientId] * grpCfg->arrival_rate);


		for(long i=0; i<numReqs; i++){
			threads.emplace_back(run_session, grpCfg->object_size, grpCfg->read_ratio, std::ref(grpCfg->keys), \
							grpCfg->placement_p, timePoint, std::ref(dist_process), grp_idx, i, desc);
		}

		timePoint += millis{grpCfg->duration* 1000};
		std::this_thread::sleep_until(timePoint);

		running = false;
		for(auto &th: threads){
			th.join();
		}

		destroy_liberasure_instance(desc);
		std::cout<<"NUm value before returning" << numt.load() << std::endl;
		avg += numt.load()/(grpCfg->duration);
		threads.clear();
		numt = 0;
		key_init = false;
	}

	avg = avg/act_cnt;
	std::cout<<"Average arrival Rate : " << avg << std::endl;
	return avg;
}

int main(int argc, char* argv[]){

	if(argc != 2){
		std::cout<<"Incorrect number of arguments: Please specify the port number" << std::endl;
		return 1;
	}
	int newSocket = socket_setup(argv[1]);
	std::cout<< "Waiting for connection from Controller." <<std::endl;
	std::cout<< "Atomic variable is lock free? " << running.is_lock_free() << " : another opinion : " << ATOMIC_BOOL_LOCK_FREE << std::endl;
	int sock = accept(newSocket, NULL, 0);
	close(newSocket);

	std::string recv_str;
	try{

		if(DataTransfer::recvMsg(sock, recv_str) != 1){
			std::cout<< "Client Config could not be received! " << std::endl;
			return 1;
		}
		close(sock);

		Properties cc = DataTransfer::deserializePrp(recv_str);
		// std::cout<< "client id " << cc.local_datacenter_id << " group " << cc.groups[0]->grp_id[1] << " arrival rate " << cc.groups[0]->grp_config[1]->arrival_rate
		// 			<< "read ratio  " << cc.groups[0]->grp_config[1]->read_ratio << "keys" << cc.groups[0]->grp_config[1]->keys[0] << std::endl;
		// std::cout<< "group " << cc.groups[1]->grp_id[1] << " arrival rate " << cc.groups[1]->grp_config[1]->arrival_rate
		// 						<< "read ratio  " << cc.groups[1]->grp_config[1]->read_ratio <<  cc.groups[1]->grp_config[1]->keys[0] << std::endl;

		if(cc.groups.empty()){
			std::cout << "No Key groups were found" <<std::endl;
			return 0;
		}

		// //TODO:: For testing purpose. Initialzing the key
		// Placement *p = cc.groups[0]->grp_config[0]->placement_p;
		// struct ec_args null_args;
	    // null_args.k = p->k;
	    // null_args.m = p->m - p->k;
	    // null_args.w = 16; // ToDo: what must it be?
	    // null_args.ct = CHKSUM_NONE;
		//
		// int desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, &null_args);
		// CAS_Client clt(test, 1, desc);
		// clt.put("group11", "1111aaaaa", *p, true);
		// clt.put("group12", "22222bbbbb", *p, true);
		//
		// assert(0 == liberasurecode_instance_destroy(desc));

		//TODO:: decide where to specify the distribiution
		// Assumes the Group 0 in config contains all key grps
		// Because initialization of all key_grps needed at start time
		// Create a separate process for each Key group
		// NOTE:: grp_id size and grp_config size should be equal for Groupxxxx
		int grp_size = cc.groups[0]->grp_id.size();
		for(int i=0; i<grp_size; i++){
			if(fork() == 0){
				int rate = key_req_gen(cc, cc.groups[0]->grp_id[i], "uniform");
				std::cout << "Rate sent is " << rate  << std::endl;
				exit(rate);
			}
		}

		float avg_rate = 0;
		int ret_val =0;
		while(wait(&ret_val) >=0){
			std::cout << "Child temination status " << WIFEXITED(ret_val) << "  Rate receved is " <<  WEXITSTATUS(ret_val) <<
					" : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) <<std::endl;
			avg_rate += WEXITSTATUS(ret_val);
		}

		std::cout<< "Final avg rate is " << avg_rate << std::endl;

	}
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
	}


	return 0;

}
