#include "Util.h"
#include <iostream>
#include "Data_Transfer.h"
#include <thread>
#include <chrono>
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
uint32_t clientId;


std::atomic<bool> running(false);
std::atomic<int> numt(0);



using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;

static inline int next_event(std::string &dist_process){
	if(dist_process == "uniform"){
		return 1000;
	}else if(dist_process == "poisson"){
		return 670;
	}else{
		throw std::logic_error("Distribution process specified is unknown !! ");
	}
	return 0;
}

int run_session(uint32_t obj_size, double read_ratio, std::vector<std::string> &keys, Placement *pp, time_point<system_clock, millis> tp,
							std::string &dist_process, int grp_idx, int req_idx){
	int cnt = 0; 		// Count the number of requests
	int reqType = 0;    // 1 for GET, 2 for PUT
	int key_idx = -1;
	uint32_t threadId = ((clientId << 24) | (grp_idx << 12) | (req_idx));
	CAS_Client clt(test, threadId);
	srand(threadId);

	std::string read_value;
	while(running.load()){
		//std::cout << "Thread created !" << std::endl;
		cnt+= 1;
		// Choose the operation type
		double random_ratio = static_cast<double>(rand() % 100) / 100.00;
		reqType = random_ratio <= read_ratio? 1:2;

		//Choose a random key
		key_idx = rand()%(keys.size());
		std::cout << "Operation chosen by Client  "<<threadId<<" for key " << keys[key_idx] << "  is " << reqType << std::endl;
		// Initiate the operation
		if(reqType == 1){
			clt.get(keys[key_idx],read_value, *pp);
		}else{
			std::string val(obj_size, 'a');
			clt.put(keys[key_idx], val, *pp, false);
		}

		tp += millis{next_event(dist_process)};
		std::this_thread::sleep_until(tp);
	}
	numt += cnt;
}

int key_req_gen(Properties &prop, int grp_idx, std::string dist_process){

	//TODO;; may need to chnage this to client _id
	clientId = prop.local_datacenter_id;
	uint64_t startTime = prop.start_time;
	time_point<system_clock, millis> timePoint(millis{startTime});

	std::cout<<"Waiting for the Start time...." << std::endl;
	std::this_thread::sleep_until(timePoint);
	std::cout << "Starting the request generation! " << std::endl;

	GroupConfig *grpCfg;
	for(int j=0; j<prop.groups.size() ; j++){
		// If the key id doesn't exist in this timestamp, skip it
		auto it = find(prop.groups[j]->grp_id.begin(), prop.groups[j]->grp_id.end(), grp_idx);
		if( it == prop.groups[j]->grp_id.end()){
			++j;
			continue;
		}

		// The config for this grp index
		grpCfg = prop.groups[j]->grp_config[it - prop.groups[j]->grp_id.begin()];

		running = true;
		//Start threads
		std::vector<std::thread> threads;
		long numReqs = lround(grpCfg->client_dist[clientId] * grpCfg->arrival_rate);

		for(long i=0; i<numReqs; i++){
			threads.emplace_back(run_session, grpCfg->object_size, grpCfg->read_ratio, std::ref(grpCfg->keys), \
							grpCfg->placement_p, timePoint, std::ref(dist_process), grp_idx, i);
		}

		std::cout<<"sleeeeeep...." << std::endl;
		timePoint += millis{grpCfg->duration* 1000};
		std::this_thread::sleep_until(timePoint);
		std::cout<<"yaaaaas...." << std::endl;

		running = false;
		for(auto &th: threads){
			th.join();
		}

		std::cout<<"total threads created  : " << numt.load()<< std::endl;
		threads.clear();

	}
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

		//TODO:: For testing purpose. Initialzing the key
		CAS_Client clt(test, 1);
		clt.put("group11", "1111aaaaa", *(cc.groups[0]->grp_config[0]->placement_p), true);
		clt.put("group12", "22222bbbbb", *(cc.groups[0]->grp_config[0]->placement_p), true);

		//TODO:: decide where to specify the distribiution
		// Create a separate process for each Key group
		for(int i=0; i<cc.groups[0]->grp_id.size(); i++){
			if(fork() == 0){
				key_req_gen(cc, cc.groups[0]->grp_id[i], "uniform");
				exit(0);
			}
		}

		while(wait(NULL) >=0);
		//TOOD:: Free up the properties structure

	}
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
	}


	return 0;

}
