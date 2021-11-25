#include "../inc/Persistent.h"
#include "Data_Transfer.h"
#include <cassert>
#include "Data_Server.h"

Persistent::Persistent(const std::string& directory){
    
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
//    options.merge_operator.reset(new Persistent_merge);
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, directory, &db);
    assert(status.ok());

    #ifdef GCS
    {

        // Remove .temp from directory
        std::size_t found_index = directory.find(".temp");
        assert(found_index!=std::string::npos);
        bucket_name = std::string(directory.substr(0,found_index));
        for(int i=0;i<15;i++)
            bucket_name += directory[found_index-1];
        // std::cerr << "Bucket name is " << bucket_name << std::endl;

        // Create gcs client with given configs
        Init_GCS_client(gcs_client, bucket_name, GCS_PROJECT_ID, GCS_STORAGE_LOCATION);
    }
    #endif
}

Persistent::~Persistent(){
    delete db;
}

//TODO:: Get the interface uniform with Cache interface
std::vector<std::string> Persistent::get(const std::string& key){
    std::string value;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
    
    #ifdef GCS
    {
        GCS_Get(gcs_client, bucket_name, key, value);
    }
    #endif

    if(s.IsNotFound()){
        DPRINTF(DEBUG_CAS_Server, "persisttent get VALUE NOT FOUND! key is %s\n", key.c_str());
        return std::vector<std::string>();
    }
    else{
        DPRINTF(DEBUG_CAS_Server, "persisttent get VALUE FOUND! key is %s :  and the value size is %zu\n", key.c_str(),
                value.size());
        return DataTransfer::deserialize(value);
//        DPRINTF(DEBUG_CAS_Server, "PERSISTENT GET~!!! -- size after decoding : %lu\n", raw_data[0].size());
//        return raw_data;
    }
}

void Persistent::put(const std::string& key, const std::vector<std::string>& value){
    
    if(value.empty()){
        DPRINTF(DEBUG_CAS_Server, "Error: emtpy insertion\n");
        assert(0);
    }
    std::string out_str = DataTransfer::serialize(value);
    
    // DPRINTF(DEBUG_CAS_Server,"PERSISTENT PUT~!!! -- size before endonig : %lu\n", value[0].size());
    // std::vector<std::string> test_dec =  DataTransfer::deserialize(out_str);
    //
    // //TODO:: do I need this anymore?
    // if(!(value[0] == test_dec[0])){
    // 	DPRINTF(DEBUG_CAS_Server,"ENCODE DECODE TEST FAILED!!!!! out str : %s and test_dec : %s\n", value[0].c_str(), test_dec[0].c_str());
    // }
    
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, out_str);
    if(!s.ok()){
        DPRINTF(DEBUG_CAS_Server, "db->Put Error: %s\n", s.ToString().c_str());
//        std::cerr << s.ToString() << std::endl;
        assert(s.ok());
    }

    #ifdef GCS
    {
        GCS_Put(gcs_client, bucket_name, key, out_str);
    }
    #endif
}

void Persistent::merge(const std::string& key, const std::vector<std::string>& value){
    if(value.empty()){
        DPRINTF(DEBUG_CAS_Server, "Error: emtpy insertion\n");
        assert(0);
    }
    std::string out_str = DataTransfer::serialize(value);
    rocksdb::Status s = db->Merge(rocksdb::WriteOptions(), key, out_str);
    if(!s.ok()){
        DPRINTF(DEBUG_CAS_Server, "db->Put Error: %s\n", s.ToString().c_str());
//        std::cerr << s.ToString() << std::endl;
        assert(s.ok());
    }
}

//bool Persistent::exists(const std::string& key){
//
//    std::string _value;
//    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &_value);
//    if(s.IsNotFound()){
//        return false;
//    }
//    else{
//        return true;
//    }
//}

//void Persistent::modify_flag(const std::string& key, int label){
//
//    std::string _value;
//    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &_value);
//    assert(s.ok());
//
//    //Modify the First Byte
//    std::vector <std::string> raw_data = DataTransfer::deserialize(_value);
//    raw_data[1] = static_cast<char>(label);
//    _value = DataTransfer::serialize(raw_data);
//
//    s = db->Put(rocksdb::WriteOptions(), key, _value);
//    if(!s.ok()){
//        std::cerr << s.ToString() << std::endl;
//        assert(s.ok());
//    }
//
//}
/*
// First Byte in the byte stream is actually the "Fin" tag
// So the value field starts from the second byte
std::string& Persistent::encode(std::vector<std::string> &value){

	if(value.size() > 1){
		value[0].insert(0,value[1]);
		std::cout<<"ENCODE:  Value actaully inserted is " << value[0] <<std::endl;
	}
	return value[0];
}

std::vector<std::string> Persistent::decode(const std::string &value){

	 std::vector<std::string> _value;
	_value.push_back(std::string(value.begin()+1, value.end()) );
	_value.push_back(std::string(value,0,1) );

	return _value;
}*/
