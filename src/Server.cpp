
#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"
#include <sys/ioctl.h>
#include <unordered_set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

//#include <linux/sockios.h>
//std::atomic<bool> active(true);
//std::atomic<bool> reconfig(false);
//std::mutex rcfglock;
//
//enum rcfg_state{
//	RECONFIG_BLOCK,
//	RECONFIG_QUERY,
//	RECONFIG_FINALIZE,
//	RECONFIG_WRITE,
//	RECONFIG_FINISH
//};

//ccv is there to ensure ordering amongst
// reconfiguration protocol msgs
//struct rcfgSet{
//	std::condition_variable rcv;
//	std::condition_variable ccv;
//	std::string highest_timestamp;
//	uint waiting_threads;
//	std::string new_cfg;
//	rcfg_state state;
//};

//Keys for which client actions are blocked
//std::unordered_set<std::string> blocked_keys;
//Maps key to it's reconfiguration meta data
//std::unordered_map<std::string, rcfgSet*> rcfgKeys;

//bool key_match(std::string curr_key){
////    curr_key.erase(curr_key.begin());
////    curr_key.erase(curr_key.begin());
////    curr_key.erase(curr_key.begin());
//    if(rcfgKeys.find(curr_key) != rcfgKeys.end()){
//            if(rcfgKeys[curr_key]->state != RECONFIG_FINISH)
//                    return true;
//    }
//    return false;
//}

//int key_add(std::string curr_key){
////    curr_key.erase(curr_key.begin());
////    curr_key.erase(curr_key.begin());
////    curr_key.erase(curr_key.begin());
//
//	if(rcfgKeys.find(curr_key) == rcfgKeys.end()){
//		rcfgKeys[curr_key] = new rcfgSet;
//		rcfgKeys[curr_key]->waiting_threads = 0;
//		rcfgKeys[curr_key]->state = RECONFIG_BLOCK;
//		return 1;
//	}else if(rcfgKeys.find(curr_key) != rcfgKeys.end() && rcfgKeys[curr_key]->state == RECONFIG_BLOCK){
//		return 1;
//	}else{ 	// Previous reconfig still not complete
//		std::cout << "Reconfiguration for the same key is already in progresss !!" << std::endl;
//		//assert(0);
//		return -1;
//	}
//}

//std::string reconfig_query(DataServer &ds, std::string &curr_key, std::string &curr_class){
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "started. for key %s\n", curr_key.c_str());
//    reconfig = true;
//    if(key_add(curr_key) == 1){
////        std::string key_c = curr_key;
////        key_c.erase(key_c.begin());
////        key_c.erase(key_c.begin());
////        key_c.erase(key_c.begin());
//        rcfgSet *config = rcfgKeys[curr_key];
//        config->state = RECONFIG_QUERY;
//        config->ccv.notify_all();
//        std::cout << "SSSSS reconfig_query is received." << std::endl;
//        return ds.reconfig_query(curr_key, curr_class);
//    }else{
//        return DataTransfer::serialize({"Failed"});
//    }
//}

//std::string reconfig_finalize(DataServer &ds, std::unique_lock<std::mutex> &lck,
//	 					std::string &key, std::string &timestamp, std::string &curr_class){
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "started. for key %s\n", key.c_str());
//    key_add(key);
////    std::string key_c = key;
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
//    rcfgSet *config = rcfgKeys[key];
//    while(config->state == RECONFIG_BLOCK){
//        config->ccv.wait(lck);
//    }
//    // state > RECONFIG_BLOCK // Todo: what is it for here?
//    return ds.reconfig_finalize(key, timestamp, curr_class);
//}


//std::string write_config(DataServer &ds, std::unique_lock<std::mutex> &lck, std::string &key,
//					std::string &value, std::string &timestamp, std::string &curr_class){
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "started. for key %s\n", key.c_str());
//    key_add(key);
////    std::string key_c = key;
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
//    rcfgSet *config = rcfgKeys[key];
//    while(config->state == RECONFIG_BLOCK){
//            config->ccv.wait(lck);
//    }
//    config->state = RECONFIG_WRITE;
//    config->ccv.notify_all();
//    return ds.write_config(key, value, timestamp, curr_class);
//
//}

//std::string finish_reconfig(std::unique_lock<std::mutex> &lck, std::string &key,
//								 std::string &timestamp, std::string &cfg){
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "started. for key %s\n", key.c_str());
//    key_add(key);
////    std::string key_c = key;
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
////    key_c.erase(key_c.begin());
//    rcfgSet *config = rcfgKeys[key];
////    while(config->state < RECONFIG_WRITE){
////        config->ccv.wait(lck);
////    }
//
//    config->state = RECONFIG_FINISH;
//    config->highest_timestamp = timestamp;
//    config->new_cfg = cfg;
//    if(rcfgKeys.empty())
//            reconfig = false;
//    config->rcv.notify_all();
//    
//    std::cout << "SSSSS finish_reconfig is received." << std::endl;
//    
//    return DataTransfer::serialize({"OK"});
//}

void server_connection(int connection, DataServer &dataserver, int portid){
    
    Request req;
    req.sock = connection;

    std::string recvd;
    int result = DataTransfer::recvMsg(connection,recvd);
    if(result != 1){
        strVec msg{"failure","No data Found"};
        DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
        close(connection);
        return;
    }

    // if data.size > 3
    // Data[0] -> method_name
    // Data[1] -> key
    // Data[2] -> timestamp
    strVec data = DataTransfer::deserialize(recvd);
    std::string &method = data[0];
    
//    if(method != "get" && method != "put" && method != "get_timestamp"){
//        DPRINTF(DEBUG_RECONFIG_CONTROL, "The method %s is called. The key is %s, server port is %u\n",
//                    method.c_str(), data[1].c_str(), portid);
//    }
    

//    std::unique_lock<std::mutex> rlock(rcfglock);
//    if(reconfig.load() && key_match(data[1])){
//        if(method != "write_config" && method != "reconfig_finalize" && method != "finish_reconfig"){
//            
//            DPRINTF(DEBUG_RECONFIG_CONTROL, "method %s is called while reconfiguration is going on"
//                    ". key is %s\n", method.c_str(), data[1].c_str());
//
//            
//            rcfgSet *config = rcfgKeys[data[1]];
//            config->waiting_threads++;
//            //Block the thread
//            while(config->state != RECONFIG_FINISH) {
//                config->rcv.wait(rlock);
//            }
//
//            config->waiting_threads--;
//            //Check which operations to service and which to reject
//            if(data.size() > 3 && Timestamp::compare_timestamp(config->highest_timestamp, data[2])){
//                //if data[2] < highest and operation is not 'get_timestamp', then service the thread
//                // Garbage collect, Reconfig complete
//                
//                DPRINTF(DEBUG_RECONFIG_CONTROL, "method %s was called while reconfiguration was going on"
//                        ". key is %s, reconfiguration is finished and because of the timestamp we fulfilled"
//                        " the operation\n", method.c_str(), data[1].c_str());
//                
//                if(config->waiting_threads == 0){
//                    delete config;
//                    rcfgKeys.erase(data[1]);
//                }
//
//            }else{
//                //Terminate the thread and send new config
//                DPRINTF(DEBUG_RECONFIG_CONTROL, "method %s was called while reconfiguration was going on"
//                        ". key is %s, reconfiguration is finished and because of the timestamp we declined"
//                        " the operation\n", method.c_str(), data[1].c_str());
//                result = DataTransfer::sendMsg(connection, DataTransfer::serialize({"operation_fail", config->new_cfg}));
//                printf("OPERATION FAILED sent out for method: %s key : %s and timestamp:%s ", data[0].c_str(), data[1].c_str(), data[2].c_str());
//                // Garbage collect, Reconfig complete
//                if(config->waiting_threads == 0){
//                    delete config;
//                    rcfgKeys.erase(data[1]);
//                }
//
//                close(connection);
//                return;
//            }
//        }
//    }

    req.function = method;
    if(method == "put"){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "The method put is called. The key is %s, ts: %s, value: %s, class: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), data[3].c_str(), data[4].c_str(), portid);
        req.key = data[1];
        req.conf_id = stoul(data[5]);
        req.timestamp = data[2];
        req.value = data[3];
        req.protocol = data[4];
        result = DataTransfer::sendMsg(connection, dataserver.put(data[1], data[3], data[2], data[4], stoul(data[5]), req));
    }else if(method == "get"){
        if(data[3] == CAS_PROTOCOL_NAME){
            DPRINTF(DEBUG_RECONFIG_CONTROL, "The method get is called. The key is %s, ts: %s, class: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), data[3].c_str(), portid);
            //std::cout << "GET fucntion called for server id "<< portid << std::endl;
            req.key = data[1];
            req.conf_id = stoul(data[4]);
            req.timestamp = data[2];
    //        req.value = data[3];
            req.protocol = data[3];
            result = DataTransfer::sendMsg(connection, dataserver.get(data[1], data[2], data[3], stoul(data[4]), req));
        }
        else{
            DPRINTF(DEBUG_RECONFIG_CONTROL, "The method get is called. The key is %s, class: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), portid);
            //std::cout << "GET fucntion called for server id "<< portid << std::endl;
            req.key = data[1];
            req.conf_id = stoul(data[3]);
            req.timestamp = "";
    //        req.value = data[3];
            req.protocol = data[2];
            std::string tt;
            result = DataTransfer::sendMsg(connection, dataserver.get(data[1], tt, data[2], stoul(data[3]), req));
        }
        
        
    }else if(method == "get_timestamp"){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "The method get_timestamp is called. The key is %s, class: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), portid);
        req.key = data[1];
        req.conf_id = stoul(data[3]);
//        req.timestamp = data[2];
//        req.value = data[3];
        req.protocol = data[2];
        result = DataTransfer::sendMsg(connection, dataserver.get_timestamp(data[1], data[2], stoul(data[3]), req));
    }else if(method == "put_fin"){
        req.key = data[1];
        req.conf_id = stoul(data[4]);
        req.timestamp = data[2];
//        req.value = data[3];
        req.protocol = data[3];
        result = DataTransfer::sendMsg(connection, dataserver.put_fin(data[1], data[2], data[3], stoul(data[4]), req));
//    }else if(method == "reconfig_query"){
//            result = DataTransfer::sendMsg(connection, reconfig_query(dataserver, data[1], data[2]));
//    }else if(method == "reconfig_finalize"){
//            result = DataTransfer::sendMsg(connection, reconfig_finalize(dataserver, rlock, data[1], data[2], data[3]));
//    }else if(method == "write_config"){
//            result = DataTransfer::sendMsg(connection, write_config(dataserver, rlock, data[1], data[3], data[2], data[4]));
//    }else if(method == "finish_reconfig"){
//            result = DataTransfer::sendMsg(connection, finish_reconfig(rlock, data[1], data[2], data[3]));
    }
    else {
            DataTransfer::sendMsg(connection,  DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
    }

    if(result != 1){
            DataTransfer::sendMsg(connection,  DataTransfer::serialize({"Failure","Server Response failed"}));
    }

    shutdown(connection, SHUT_WR);
    close(connection);
}


void runServer(std::string &db_name, std::string &socket_port){

	DataServer *ds = new DataServer(db_name, socket_setup(socket_port));
        int portid = stoi(socket_port);
        std::cout<<"Alive port "<<portid<< std::endl;
	while(1){
		int new_sock = accept(ds->getSocketDesc(), NULL, 0);
                std::cout<<"Received Request!!1  PORT:" <<portid<<std::endl;
		std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid);});
		cThread.detach();
	}

}

int main(int argc, char **argv){

	std::vector<std::string> socket_port;
	std::vector<std::string> db_list;

	if(argc == 1){
		socket_port = {"10000", "10001", "10002", "10003","10004","10005","10006","10007","10008"};
		db_list = {"db1.temp", "db2.temp", "db3.temp", "db4.temp", "db5.temp", "db6.temp", "db7.temp", "db8.temp", "db9.temp"};
	}else if(argc == 3){
		socket_port.push_back(argv[1]);
		db_list.push_back(std::string(argv[2]) + ".temp");
	}else{
		std::cout << "Enter the correct number of arguments :  ./Server <port_no> <db_name>" << std::endl;
		return 0;
	}


	for(uint i=0; i< socket_port.size(); i++){
		if(fork() == 0){
                    
                    close(1);
                    int pid = getpid();
                    std::stringstream filename;
                    filename << "server_" << pid << "_output.txt";
                    fopen(filename.str().c_str(), "w");
                    runServer(db_list[i], socket_port[i]);
		}
	}

	std::string ch;
	//Enter quit to exit the thread
	while(ch != "quit"){
		std::cin >> ch;
	}

	std::cout<<"Waiting for all detached threads to terminate!" << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(5));

	return 0;
}
