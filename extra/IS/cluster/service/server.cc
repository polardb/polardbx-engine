#include <string>
#include "../service/state_machine_server.h"
#include <easy_io.h>
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <signal.h>
#include "raft.h"

DECLARE_string(rpc_members);
DECLARE_string(server_members);
DECLARE_int32(server_id);

using namespace alisql;

static volatile bool sQuit= false;
static void SignalIntHandler(int /*sig*/)
{
  sQuit= true;
}

int main(int argc, char* argv[])
{
  int ret;
  google::ParseCommandLineFlags(&argc, &argv, true);
  std::string host_name;
  std::vector<std::string> serverMembers;
  boost::split(serverMembers, FLAGS_server_members,
               boost::is_any_of(","), boost::token_compress_on);

  if (serverMembers.size() == 0)
  {
    LOG_INFO("server members is empty , please check your configuration\n");
    return -1;
  }

  std::vector<std::string> rpcMembers;
  boost::split(rpcMembers, FLAGS_rpc_members,
               boost::is_any_of(","), boost::token_compress_on);

  if (rpcMembers.size() == 0)
  {
    LOG_INFO("rps members is empty , please check your configuration\n");    
    return -1;
  }

  if (FLAGS_server_id < 1 ||
      FLAGS_server_id > static_cast<int32_t>(serverMembers.size()))
  {
    LOG_INFO("incorrect server_id: %d\n", FLAGS_server_id);
    return -1;
  }

  std::string serverId= serverMembers.at(FLAGS_server_id - 1);

  StateMachineServer *server= new StateMachineServer();

  server->init(serverId, serverMembers, rpcMembers);

  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  while (!sQuit)
  {
   	sleep(1);
  }

  server->shutdown();
  delete server;

  return ret;
}


