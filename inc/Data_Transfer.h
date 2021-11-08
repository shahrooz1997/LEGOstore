#ifndef _DATA_TRANSFER_H_
#define _DATA_TRANSFER_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <string>
#include <arpa/inet.h>
#include "gbuffer.pb.h"
#include "Util.h"
#include <cerrno>
#include <mutex>
#include <future>

typedef std::vector<std::string> strVec;

class DataTransfer {
 public:
  static int sendAll(int sock, const void *data, int data_size);

  static int sendMsg(int sock, const std::string &out_str);

  static int recvAll(int sock, void *buf, int data_size);

  static int recvMsg(int sock, std::string &data);

  static int recvMsg_async(const int sock, std::promise<std::string> &&data_set);

  static std::string serialize(const strVec &data);

//    static std::string serializePrp(const Properties& properties_p);

//    static std::string serializePlacement(const Placement& placement);

  static strVec deserialize(const std::string &data);

//    static Properties* deserializePrp(std::string& data);

//    static Placement* deserializePlacement(const std::string& data);

//    static std::string serializeCFG(const Placement& pp);

//    static Placement deserializeCFG(std::string& data);


  static std::string serializeMDS(const std::string &status,
                                  const std::string &msg,
                                  const std::string &key,
                                  const uint32_t &curr_conf_id,
                                  const uint32_t &new_conf_id,
                                  const std::string &timestamp,
                                  const Placement &placement);

  static std::string serializeMDS(const std::string &status,
                                  const std::string &msg,
                                  const std::string &key = "",
                                  const uint32_t &curr_conf_id = 0,
                                  const uint32_t &new_conf_id = 0,
                                  const std::string &timestamp = "");

  static Placement deserializeMDS(const std::string &data, std::string &status, std::string &msg, std::string &key,
                                  uint32_t &curr_conf_id, uint32_t &new_conf_id, std::string &timestamp);

  static Placement deserializeMDS(const std::string &data, std::string &status, std::string &msg);

  static int deserializeOperation(const std::string &str_in,
                                  std::unique_ptr<packet::Operation::MethodType> &method_p,
                                  std::unique_ptr<std::vector<std::string>> &data_p,
                                  std::unique_ptr<std::string> &msg_p,
                                  std::unique_ptr<std::string> &key_p,
                                  std::unique_ptr<std::string> &timestamp_p,
                                  std::unique_ptr<std::string> &value_p,
                                  std::unique_ptr<packet::Operation::AlgorithmName> &algorithm_name_p,
                                  std::unique_ptr<uint32_t> &conf_id_p,
                                  std::unique_ptr<uint32_t> &new_conf_id_p);

  static int serializeToOperation(const packet::Operation::MethodType &method,
                                  const std::vector<std::string> &data,
                                  const std::string &msg,
                                  const std::string &key,
                                  const std::string &timestamp,
                                  const std::string &value,
                                  const packet::Operation::AlgorithmName &algorithm_name,
                                  const uint32_t &conf_id,
                                  const uint32_t &new_conf_id,
                                  packet::Operation &operation);

  static std::string serializeToOperation(const packet::Operation::MethodType &method,
                                  const std::vector<std::string> &data,
                                  const std::string &msg,
                                  const std::string &key,
                                  const std::string &timestamp,
                                  const std::string &value,
                                  const packet::Operation::AlgorithmName &algorithm_name,
                                  const uint32_t &conf_id,
                                  const uint32_t &new_conf_id);

  static int serializeToOperation(const packet::Operation::MethodType &method,
                                  const std::string &msg,
                                  packet::Operation &operation);

  static std::string serializeToOperation(const packet::Operation::MethodType &method,
                                  const std::string &msg);
};

#endif
