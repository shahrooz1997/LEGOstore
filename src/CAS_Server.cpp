/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   CAS_Server.cpp
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#include "CAS_Server.h"

using std::string;

CAS_Server::CAS_Server() {
}


CAS_Server::~CAS_Server() {
}

std::string CAS_Server::get_timestamp(string &key, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	//std::lock_guard<std::mutex> lock(lock_t);
	DPRINTF(DEBUG_CAS_Server, "get_timestamp started and the key is %s\n", key.c_str());
	const std::vector<std::string> *ptr = cache.get(key);
	strVec data; // = (*cache.get(key))[0];
	strVec result;

	if(ptr == nullptr){
		data = persistent.get(key);
		DPRINTF(DEBUG_CAS_Server,"cache data checked and data is null\n");
		if(data.empty()){
			result = {"Failed", "None"};

		}else{
			DPRINTF(DEBUG_CAS_Server,"Found data in persisten. sending back! data is"\
				"key:%s   timestamp:%s\n", key.c_str(), data[0].c_str() );
			result = {"OK", data[0]};
		}
	}else{
		DPRINTF(DEBUG_CAS_Server,"cache data checked and data is found\n");
		data = (*ptr);
		DPRINTF(DEBUG_CAS_Server,"cache data checked and data is %s\n",data[0].c_str() );
		result = {"OK", data[0]};
	}

	assert(result.empty() != 1);
	return DataTransfer::serialize(result);
}

std::string CAS_Server::put(string &key, string &value, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	//std::lock_guard<std::mutex> lock(lock_t);
	DPRINTF(DEBUG_CAS_Server,"put function timestamp %s\n", timestamp.c_str());
	insert_data(key, value, timestamp, false, cache, persistent);
	return DataTransfer::serialize({"OK"});
}

std::string CAS_Server::put_fin(string &key, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	//std::lock_guard<std::mutex> lock(lock_t);
	DPRINTF(DEBUG_CAS_Server,"Calling put_fin fucntion timestamp:%s\n", timestamp.c_str());
	insert_data(key, std::string(), timestamp, true, cache, persistent);
	return DataTransfer::serialize({"OK"});
}

// Returns true if success, i.e., timestamp added as the fin timestamp for the key
bool complete_fin(std::string &key, std::string &timestamp, Cache &cache, Persistent &persistent){

	strVec data;
	bool fnd = cache.exists(key);
	if(!fnd){
		fnd = persistent.exists(key);
		if(!fnd){
			cache.put(key, {timestamp});
			persistent.put(key, {timestamp});
			return true;
		}
		data = persistent.get(key);
	}else{
		data = *cache.get(key);
	}
	// Check if the WRITE can finish
	Timestamp curr_time(timestamp);
	Timestamp saved_time(data[0]);

	if( curr_time > saved_time){
		cache.put(key, {timestamp});
		persistent.put(key, {timestamp});
		DPRINTF(DEBUG_CAS_Server,"Complete fin called with key : %s,  curr_timestsamp : %s , and saved timestsamp is : %s \n",
						key.c_str(), timestamp.c_str(), data[0].c_str());
		return true;
	}

	return false;
}

//TOD0:: when fin tag with empty value inserted, how is that handled at server for future requests,
// as in won't the server respond with emoty values when requested again.
std::string CAS_Server::get(string &key, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	//std::unique_lock<std::mutex> lock(lock_t);
	DPRINTF(DEBUG_CAS_Server,"GET function called !! \n");
	strVec result;

	bool fnd = cache.exists(key+timestamp);
	if(!fnd){
		fnd = persistent.exists(key+timestamp);
		if(!fnd){
			//insert_data(key, "None", timestamp, true, cache, persistent);
			complete_fin(key, timestamp, cache, persistent);
			result = {"Failed", "None"};
		}

		strVec data = persistent.get(key+timestamp);
		// Put back in cache
		cache.put(key+timestamp, data);
		// Update the FIN label as well as label flag
		insert_data(key,std::string(), timestamp, true, cache, persistent);
		//complete_fin(key, timestamp, cache, persistent);
		result = {"OK", data[0]};
	}

	insert_data(key, std::string(), timestamp, true, cache, persistent);
	DPRINTF(DEBUG_CAS_Server," GET value found in Cache \n");
	result = {"OK", (*cache.get(key+timestamp))[0]};

	assert(result.empty() != 1);
	return DataTransfer::serialize(result);
}


//Add client ID
void CAS_Server::insert_data(string &key,const string &val, string &timestamp, bool label, Cache &cache, Persistent &persistent){

	int _label = label? 1:0;

	//TODO:: ALready in FIN, so no way that we need to store naything , cos no way values
	// can come now in a msg right???
	//TODO:: should I add it to cache on write, from persistent
	//If not found, that means either tuple not present
	// OR tuple is in pre stage
	// chekcing to set FIN flag
//	bool fnd = cache.exists(key+timestamp);

//	if(!fnd){
//		fnd = persistent.exists(key+timestamp);

		// If Tag not seen before, add the tuple
//		if(!fnd){
			if(!val.empty()){
				std::vector<string> value{val, std::to_string(_label)};

				DPRINTF(DEBUG_CAS_Server,"timestamp : %s is being written\n", timestamp.c_str());
				///TODO:: optimize and create threads here
				// For thread, add label check here and then return
				DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
				// Also add fin tag to both
				cache.put(key+timestamp, value);
				persistent.put(key+timestamp, value);
			}
			if(label){
				complete_fin(key, timestamp, cache, persistent);
				//cache.put(key, {timestamp});
				//persistent.put(key, {timestamp});
			}
			return;
//		}
		DPRINTF(DEBUG_CAS_Server,"TAG found in persistent\n ");
		//TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
//	}

	//TODO:: Before fin, check the key if you can infact finish the write and check python code for same.
	if(label){
		// TODO:: check if not found, does LAbel update make sense??
		DPRINTF(DEBUG_CAS_Server,"ADDING the FIN tag to key %s timestamp:%s\n", key.c_str(), timestamp.c_str());
		//cache.put(key, {timestamp});
		//persistent.put(key, {timestamp});
		//std::cout<<"put_fin value read back is" <<  persistent.get(key)[0] << std::endl;
		// Use modify function, cos don't want to overwrite value with NULL
		// Take care of case where data not in Cache
		cache.modify_flag(key+timestamp,_label);
		persistent.modify_flag(key+timestamp, _label);
		complete_fin(key, timestamp, cache, persistent); // What is this for??
	}

}
