#include "Persistent.h"

Persistent::Persistent(const std::string &directory){

	rocksdb::Options options;
	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  	options.IncreaseParallelism();
  	options.OptimizeLevelStyleCompaction();
	options.create_if_missing = true;
	rocksdb::Status status = rocksdb::DB::Open(options, directory, &db);
	assert(status.ok());
}	

//TODO:: Get the interface uniform with Cache interface
const std::vector<std::string> Persistent::get(const std::string &key){
	std::string value;
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
	if(s.IsNotFound()){
		std::cout<<"persisttent get VALUE NOT FOUND! key is" << key<<std::endl;
		return std::vector<std::string>();
	}else{
		std::cout<<"persisttent get VALUE FOUND! key is" << key<<"Value is"<< value <<std::endl;
		return decode(value);
	}
}

void Persistent::put(const std::string &key, std::vector<std::string> value){

//	std::cout<<"INsertig value into PERSISTENT :"<< value[1] <<" : " <<value[0] <<std::endl;	
	rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, encode(value) );
	if(!s.ok()){
		std::cerr << s.ToString() << std::endl;
		assert(s.ok());
	}	
}

bool Persistent::exists(const std::string &key){

	std::string _value;
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &_value);
	if(s.IsNotFound()){
		return false;
	}else{
		return true;
	}
}

void Persistent::modify_flag(const std::string &key, int label){

	std::string _value;
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &_value);
	assert(s.ok());
	
	//Modify the First Byte
	_value[0] = static_cast<char>(label);
	s = db->Put(rocksdb::WriteOptions(), key, _value);
	if(!s.ok()){
		std::cerr << s.ToString() << std::endl;
		assert(s.ok());
	}		

}

// First Byte in the byte stream is actually the "Fin" tag
// So the value field starts from the second byte
std::string& Persistent::encode(std::vector<std::string> &value){
	
	value[0].insert(0,value[1]);
	std::cout<<"ENCODE:  Value actaully inserted is " << value[0] <<std::endl;
	return value[0];
}

std::vector<std::string> Persistent::decode(const std::string &value){
	
	 std::vector<std::string> _value;
	_value.push_back(std::string(value.begin()+1, value.end()) );
	_value.push_back(std::string(value,0,1) );

	return _value;
}
