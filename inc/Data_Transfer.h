#ifndef _DATA_TRANSFER_H_
#define _DATA_TRANSFER_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <string>
#include <arpa/inet.h>
#include "serialize.pb.h"
#include "Util.h"
#include <cerrno>

typedef std::vector<std::string> strVec;

class DataTransfer{
public:
	static int sendAll(int &sock, const void *data, int data_size);

	static int sendMsg(int sock,const std::string &out_str);

	static int recvAll(int &sock, void *buf, int data_size);

	static int recvMsg(int sock, std::string &data);

	static std::string serialize(const strVec &data);

	static std::string serializePrp(const Properties &properties_p);
        
        static std::string serializePlacement(const Placement &placement);

	static strVec deserialize(std::string &data);

	static Properties* deserializePrp(std::string &data);
        
        static Placement* deserializePlacement(std::string &data);

	static std::string serializeCFG(const Placement &pp);

	static Placement deserializeCFG(std::string &data);

};


#endif
