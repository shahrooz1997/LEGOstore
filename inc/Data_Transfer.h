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

typedef std::vector<std::string> valueVec;

class DataTransfer{
public:
	static int sendAll(int &sock, const void *data, int data_size);

	static int sendMsg(int &sock,const valueVec &data);

	static int recvAll(int &sock, void *buf, int data_size);

	static int recvMsg(int sock, valueVec &in_data);

	static inline void encode(const valueVec &data, std::string *out_str);

	static inline void decode(std::string &data, valueVec &out_data);
};


#endif
