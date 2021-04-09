#!/usr/bin/python3

import command
import subprocess
from subprocess import PIPE
import threading
import os, shutil
from time import sleep
from signal import signal, SIGINT
import urllib.request
import argparse
import run_optimizer as optimizer
from collections import OrderedDict
import json

server_type = "custom-1-1024" # "n1-standard-1" # "f1-micro" #"e2-standard-2"
client_type = "e2-standard-8"
controller_type = "e2-standard-2"


weak_vm_types = ["custom-1-1024", "n1-standard-1", "f1-micro", "g1-small"]

# Please note that to use this script you need to install gcloud, login, and set the default project to Legostore.

#Todo: Add command line parser
def parse_args():
    parser = argparse.ArgumentParser(description="This application automatically runs the Legostore project on Google Cloud Platform")
    parser.add_argument('-c','--only-create', dest='only_create', action='store_true', required=False)
    parser.add_argument('-o', '--run-optimizer', dest='run_optimizer', action='store_true', required=False)
    parser.add_argument('-l', '--only-latency', dest='only_latency', action='store_true', required=False)
    parser.add_argument('-g', '--only-gather-data', dest='only_gather_data', action='store_true', required=False)
    parser.add_argument('-b', '--no-build', dest='no_build', action='store_true', required=False)

    # Manual run: Run the server on all the machines and initialize the metadata servers

    args = parser.parse_args()
    if args.only_latency or args.only_gather_data:
        args.only_create = True

    if args.only_gather_data:
        args.run_optimizer = False
    return args

# Reading zones to create the servers
def read_zones(zones_file_name='zones.txt'):
    zones_file = open(zones_file_name, "r")
    zones = []
    counter = 0
    for line in zones_file.readlines():
        # print(line)
        if (line[0] != "#" and line[0] != "\n"):
            zones.append(line[0:line.index(' ')]);
        if (line[0] == "\n"):
            counter += 1
            if (counter == 2):
                break
        else:
            counter = 0

    return zones

files_lock = threading.Lock()
def create_project_tar_file():
    files_lock.acquire()
    os.system("./copy.sh")

def delete_project_tar_file():
    os.system("rm -rf project2.tar.gz project")
    files_lock.release()

link_lock = threading.Lock()
link = ""
link_set = False
def getting_librocksdb_download_link():
    # Source 1
    # headers = {'Referer': 'https://anonfiles.com/',
    #            'Host': 'anonfiles.com',
    #            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0'}
    # url = 'https://anonfiles.com/P7Tet126p1/librocksdb_a'
    # req = urllib.request.Request(url, headers=headers)
    # page = urllib.request.urlopen(req).read().decode('utf-8')
    # link = page[page.find('download-url'):page.find('>', page.find('download-url'))]
    # link = link[link.find('href'):]
    # link = link[link.find('"') + 1:link.find('"', link.find('"') + 1)]

    # Source 2

    global link
    global link_set
    link_lock.acquire()
    if not link_set:
        os.system("rm -f /tmp/LEGOSTORE_LIBROCKSDB_LINK.txt")
        os.system("curl -Lqs \"https://www.mediafire.com/file/bs5ob9lw3urrm6g/librocksdb.a/file\" | grep \"href.*download.*media.*\" | tail -1 | cut -d '\"' -f 2 > /tmp/LEGOSTORE_LIBROCKSDB_LINK.txt")

        with open("/tmp/LEGOSTORE_LIBROCKSDB_LINK.txt") as link_file:
            link = link_file.readlines()[0][:-1]
            link_set = True
    link_lock.release()

    return link

def summarize():
    os.system("cd data; ./summarize.py >CAS_NOF/summary.txt 2>&1; cat CAS_NOF/summary.txt")

class Machine:

    existing_info = ''
    existing_info_lock = threading.Lock()

    def get_machine_list(number_of_machines=9):
        def create_machine_thread_helper(machines, m_name, m_type, m_zone):
            machines[m_name] = Machine(name=m_name, type=m_type, zone=m_zone)
        zones = command.read_zones()
        threads = []
        servers = OrderedDict()
        clients = OrderedDict()
        for i, zone in enumerate(zones):
            if(i >= number_of_machines):
                break;
            name = "s" + str(i)
            threads.append(threading.Thread(target=create_machine_thread_helper, args=[servers, name, server_type, zone]))
            threads[-1].start()

            name = "s" + str(i) + "c"
            threads.append(threading.Thread(target=create_machine_thread_helper, args=[clients, name, client_type, zone]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        servers = list(servers.items())
        servers.sort(key=lambda x: x[0])
        servers = OrderedDict(servers)

        clients = list(clients.items())
        clients.sort(key=lambda x: x[0])
        clients = OrderedDict(clients)

        print("All machines are ready to use")
        return servers, clients

    def execute_on_all(machines, cmd):
        # print("Executing " + cmd)
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.execute, args=[cmd]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def copy_to_all(machines, file, to_file):
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.copy_to, args=[file, to_file]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def dump_machines_ip_info(servers, clients):
        file = open('setup_config.json', 'w')
        file.write('{\n')

        for i in range(0, len(servers)):
            server_name = "s" + str(i)
            client_name = server_name + "c"
            server = servers[server_name]
            client = clients[client_name]
            ret = ""
            ret += '    "' + server.name[1] + '": {\n'
            ret += '        "metadata_server": {\n'
            ret += '            "host": "' + server.external_ip + '",\n'
            ret += '            "port": "30000"\n'
            ret += '        },\n'
            ret += '        "servers": {\n'
            ret += '            "1": {\n'
            ret += '                "host": "' + server.external_ip + '",\n'
            ret += '                "port": "10000"\n'
            ret += '            }\n'
            ret += '        },\n'
            ret += '        "clients": {\n'
            ret += '            "1": {\n'
            ret += '                "host": "' + client.external_ip + '"\n'
            ret += '            }\n'
            ret += '        }\n'
            if i != len(servers) - 1:
                ret += '    },\n'
            else:
                ret += '    }\n'
            file.write(ret)

        file.write('}')
        file.close()

        os.system("cp setup_config.json ../config/auto_test/datacenters_access_info.json")

    def get_pairwise_latencies(servers, clients):
        os.system("rm -f pairwise_latencies/latencies.txt")

        threads = []
        for name, client in clients.items():
            threads.append(threading.Thread(target=client.get_latencies, args=[servers]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        os.system("cd pairwise_latencies; ./combine.sh")
        print("Pairwise latencies are ready in pairwise_latencies/latencies.txt")

    def run_all(servers, clients): # if you call this method, you should not call run method anymore. It does not make sense to run twice!
        create_project_tar_file()
        threads = []
        for name, server in servers.items():
            threads.append(threading.Thread(target=server.run, args=[]))
            threads[-1].start()

        for name, client in clients.items():
            threads.append(threading.Thread(target=client.config, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        delete_project_tar_file()

        print("All machines are running")

    def stop_all(machines):
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.stop, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def clear_all(machines):
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.clear, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def delete_all(machines):
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.delete, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_summary_all(clients):
        threads = []
        os.system("mkdir -p data/" + "CAS_NOF")
        if not os.path.exists("/tmp/LEGOSTORE_AUTORUN/config"):
            print("Warn: /tmp/LEGOSTORE_AUTORUN/config directory does not exist.")
        else:
            shutil.copytree("/tmp/LEGOSTORE_AUTORUN/config", "./data/CAS_NOF/config")
        for name, client in clients.items():
            threads.append(threading.Thread(target=client.gather_summary, args=["CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_logs_all(machines):
        threads = []
        for name, machine in machines.items():
            threads.append(threading.Thread(target=machine.gather_logs, args=["CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def __init__(self, **kwargs):  # Retrieve info if machine exists, create OW
        self.name = kwargs['name']
        self.type = kwargs['type']
        self.zone = kwargs['zone']

        Machine.existing_info_lock.acquire()
        info_line = ''
        for line in Machine.existing_info.split('\n'):
            line = " ".join(line.split())
            if self.name == line.split(' ')[0]:
                info_line = line
                break

        if info_line == '':
            Machine.existing_info = command.get_instances_info()
            for line in Machine.existing_info.split('\n'):
                line = " ".join(line.split())
                if self.name == line.split(' ')[0]:
                    info_line = line
                    break
        Machine.existing_info_lock.release()

        if info_line == '':
            command.start_server(self.name, self.type, self.zone)
            print("Machine " + self.name + " created")
            sleep(20)  # Wait for the machine to boot up
            Machine.existing_info_lock.acquire()
            Machine.existing_info = command.get_instances_info()
            for line in Machine.existing_info.split('\n'):
                line = " ".join(line.split())
                if self.name == line.split(' ')[0]:
                    info_line = line
                    break
            Machine.existing_info_lock.release()

        if info_line == '':
            raise Exception("Error in creating server and setting infor for " + name)

        self.init_config()

        self.zone = info_line.split(' ')[1]
        self.type = info_line.split(' ')[2]
        self.internal_ip = info_line.split(' ')[3]
        self.external_ip = info_line.split(' ')[4]

        print("Machine " + self.name + " is ready to use")

    def execute(self, cmd):
        # print("executing " + cmd + " on server " + self.name)
        stdout, stderr = command.execute(self.name, self.zone, cmd)
        # print("output: " + stdout + " || " + stderr)
        return stdout, stderr

    def copy_to(self, file, to_file=""):
        command.scp_to(self.name, self.zone, file, to_file)

    def copy_from(self, file, to_file):
        command.scp_from(self.name, self.zone, file, to_file)

    def disk_resize(self, size_gb=20):
        command.disk_resize(self.name, self.zone, size_gb)

    def init_config(self):
        stdout, stderr = self.execute("ls ../init_config_done")
        if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
            print("Installing prerequisites on machine", self.name + "...")
            self.execute(
                "sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git protobuf-compiler pkg-config psmisc bc aria2 libgflags-dev cmake librocksdb-dev >/dev/null 2>&1")
            self.execute("git clone https://github.com/openstack/liberasurecode.git >/dev/null 2>&1")
            if self.type in weak_vm_types:
                self.execute(
                    "cd liberasurecode/; ./autogen.sh >/dev/null 2>&1; ./configure --prefix=/usr >/dev/null 2>&1; make >/dev/null 2>&1")
            else:
                self.execute(
                "cd liberasurecode/; ./autogen.sh >/dev/null 2>&1; ./configure --prefix=/usr >/dev/null 2>&1; make -j 4 >/dev/null 2>&1")
            self.execute("cd liberasurecode/; sudo make install >/dev/null 2>&1");

            # self.execute("git clone https://github.com/facebook/rocksdb.git >/dev/null 2>&1")
            # if self.type in weak_vm_types:
            #     self.execute(
            #     "cd rocksdb/; mkdir mybuild; cd mybuild; cmake ../ >/dev/null 2>&1; make USE_RTTI=1 MAKECMDGOALS=release >/dev/null 2>&1")
            # else:
            #     self.execute(
            #         "cd rocksdb/; mkdir mybuild; cd mybuild; cmake ../ >/dev/null 2>&1; make USE_RTTI=1 MAKECMDGOALS=release -j 4 >/dev/null 2>&1")
            # self.execute("cd rocksdb/mybuild; sudo make install >/dev/null 2>&1");

            # self.copy_to("../lib/librocksdb.a", "")
            # link = getting_librocksdb_download_link()
            # self.execute("aria2c -x 5 -s 5 " + link + ">/dev/null 2>&1")
            # self.execute("link=$(curl -Lqs \"https://www.mediafire.com/file/bs5ob9lw3urrm6g/librocksdb.a/file\" | grep \"href.*download.*media.*\" | tail -1 | cut -d '\"' -f 2); aria2c -x 5 -s 5 $link")
            # stdout, stderr = self.execute("ls librocksdb.a.aria2")
            # if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
            #     print("Error in downloading librocksdb.a on server " + self.name)
            # else:
            #     self.execute("sudo mv librocksdb.a ../")

            # increase the number of open files per user
            self.execute("sudo bash -c 'printf \"* hard nofile 97816\\n* soft nofile 97816\\n\" >> /etc/security/limits.conf'")
            self.execute("sudo touch ../init_config_done")

    def config(self, make_clear=True, clear_all=False):
        self.stop()
        if clear_all:
            self.clear()
        print("sending and compiling the project on machine " + self.name)
        project_tar_file_created = False
        if not os.path.exists("project2.tar.gz"):
            create_project_tar_file()
            project_tar_file_created = True
        self.copy_to("project2.tar.gz", "")
        if project_tar_file_created:
            delete_project_tar_file()

        self.execute("tar -xzf project2.tar.gz")
        # stdout, stderr = self.execute("ls project/lib/librocksdb.a")
        # if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
        #     self.execute("cp ../librocksdb.a ./project/lib/")

        if make_clear:
            if self.type in weak_vm_types:
                self.execute("cd project/; sudo make cleanall; make > /dev/null 2>&1")
            else:
                self.execute("cd project/; sudo make cleanall; make -j 4 > /dev/null 2>&1")
        else:
            if self.type in weak_vm_types:
                self.execute("cd project/; make > /dev/null 2>&1")
            else:
                self.execute("cd project/; make -j 4 > /dev/null 2>&1")

    def get_latencies(self, machines):
        self.copy_to("./myping", "")
        command = "./myping " + self.name[1:] + " "
        for name, machine in machines.items():
            command += machine.external_ip + " "

        command = command[:-1]
        self.execute(command)
        os.system("rm -f pairwise_latencies/latencies_from_server_" + self.name[1:] + ".txt")
        self.copy_from("latencies_from_server_" + self.name[1:] + ".txt", "pairwise_latencies")
        for i in range(9):
            self.copy_from("lats_from_server_" + self.name[1:] + "_to_server_" + str(i) + ".txt", "pairwise_latencies")


    def clear(self):
        self.execute("rm -rf *")

    def make_clear(self):
        self.execute("cd project/; sudo make cleanall")

    def delete(self):
        command.delete_server(self.name, self.zone)

    def run(self):
        self.stop()
        self.config()
        self.execute("cd project/; sudo make cleandb >/dev/null 2>&1")
        server_thread = threading.Thread(target=Machine.execute, args=[self, "cd project/; ./Server " + \
                                                                       self.internal_ip + " 10000 db " + self.internal_ip + " 30000 >server_output.txt 2>&1"])
        server_thread.daemon = True
        server_thread.start()
        metadata_server_thread = threading.Thread(target=Machine.execute,
                                                  args=[self, "cd project/; ./Metadata_Server " + \
                                                        self.internal_ip + " 30000 >metadata_output.txt 2>&1"])
        metadata_server_thread.daemon = True
        metadata_server_thread.start()
        while (True):
            stdout, stderr = self.execute("ps aux | grep Server | wc -l")
            if int(stdout) > 8:
                stdout, stderr = self.execute("ps aux | grep Server")
                print(stdout)
                raise Exception("Machine is already running")
            if int(stdout) > 5:
                break;
            sleep(1)

        print("server and metadata_server are running on " + self.name)

    def stop_server(self):
        self.execute("killall Server")

    def stop_metadata_server(self):
        self.execute("killall Metadata_Server")

    def stop(self):
        self.execute("sudo killall Server Metadata_Server Controller Client make cc1plus")
        # self.stop_server()
        # self.stop_metadata_server()

    def gather_summary(self, run_name):
        self.execute("cd project/; ./summarize.py " + run_name + " >sum.txt 2>&1")
        os.system("mkdir -p data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]))
        self.copy_from("project/sum.txt", "data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/")

        os.system("mkdir -p data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/logs")
        self.copy_from("project/logs/*", "data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/logs/")

    def gather_logs(self, run_name):
        os.system("mkdir -p data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]))
        if self.name[-1] != 'c':
            self.copy_from("project/server*_output.txt",
                           "data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/")
            self.copy_from("project/metadata*_output.txt",
                           "data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/")
        else:
            self.copy_from("project/client*_output.txt",
                           "data/" + run_name + "/" + (self.name if self.name[-1] != 'c' else self.name[:-1]) + "/")



# Signal handlers
keyboard_int_handler_called = False
def keyboard_int_handler(signal_received, frame):
    global keyboard_int_handler_called

    if keyboard_int_handler_called:
        print("Please Wait...")
    else:
        keyboard_int_handler_called = True
        print("Please wait while I am stopping all the machines and gathering the logs...")
        Machine.stop_all()
        Machine.gather_summary_all()
        Machine.gather_logs_all()
        exit(0)

class Controller(Machine):
    def __init__(self):
        self.created = False
        self.add_access_done = False
        def create_controller(self):
            Machine.__init__(self, name='s7-cont', type=controller_type, zone='us-west2-a')
            # self.cont_machine = Machine(name='s7-cont', zone='us-west2-a')
        self.creator_thread = threading.Thread(target=create_controller, args=[self])
        self.creator_thread.start()

    def is_created(self):
        if(self.created):
            return True
        self.creator_thread.join()
        self.created = True
        return self.created

    def add_access_to_clients(self, clients):
        self.is_created()
        stdout, stderr = self.execute("ls ../access_added")
        if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
            print("Adding access to the clients from server", self.name)
            self.execute("ssh-keygen -q -t rsa -N '' <<< \"\"$'\n'\"y\" 2>&1 >/dev/null")
            stdout, stderr = self.execute("cat .ssh/id_rsa.pub")
            Machine.execute_on_all(clients, "echo '" + stdout + "' >>.ssh/authorized_keys")
            self.execute("sudo touch ../access_added")
        self.add_access_done = True

    def run(self, clients):
        self.is_created()
        if not self.add_access_done:
            self.add_access_to_clients(clients)
        self.config()
        self.execute("cd project/; make cleandb >/dev/null 2>&1")
        print("Controller is running on " + self.name)
        self.execute('cd project/; ./Controller >controller_output.txt 2>&1')

    def gather_summary(self, run_name):
        pass

    def gather_logs(self, run_name):
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/controller_output.txt", "data/" + run_name + "/" + self.name + "/")

def make_sure_project_can_be_built():
    if os.system("cd ..; make cleanall >/dev/null 2>&1; make -j 9 >/dev/null 2>&1") != 0:
        print("Compile ERROR")
        os.system("cd ..; make")
        exit(-1)
    else:
        print("Project was built successfully")

def should_gather_outputs(controller):
    stdout, stderr = controller.execute("cat project/controller_output.txt | grep \"Child temination status 0\"")
    if stdout != "":
        return True
    return False

def main(args):
    # signal(SIGINT, keyboard_int_handler)
    if not args.only_create and not args.no_build:
        make_sure_project_can_be_built()

    if args.run_optimizer:
        optimizer.main()
        return

    controller = Controller()
    servers, clients = Machine.get_machine_list()
    Machine.dump_machines_ip_info(servers, clients)
    machines = OrderedDict(list(servers.items()) + list(clients.items()) + list([(controller.name, controller)]))

    # Machine.execute_on_all(machines, "sudo bash -c 'printf \"* soft stack 1024\\n\" >> /etc/security/limits.conf'")
    # for name, machine in machines.items():
    #     machine.copy_from("project/aaa.txt", "CAS_NOF/" + machine.name)
    # return

    if args.only_latency:
        Machine.get_pairwise_latencies(servers, clients)

    if not args.only_create:
        Machine.run_all(servers, clients)
        controller.run(clients)
        print("Project execution finished.\nPlease wait while I am stopping all the machines and gathering the logs...")
        os.system("rm -rf /home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/CAS_NOF")
        Machine.stop_all(machines)
        Machine.gather_summary_all(clients)
        summarize()
        # if should_gather_outputs(controller):
        #     print("WARN: There has been an error in at least one client. Gathering logs...")
        #     Machine.gather_logs_all(machines)
        Machine.gather_logs_all(machines)

    if args.only_gather_data:
        print("Please wait while I am gathering the logs...")
        # os.system("rm -rf /home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/CAS_NOF")
        Machine.stop_all(machines)
        # Machine.gather_summary_all(clients)
        # summarize()
        # if should_gather_outputs(controller):
        #     print("WARN: There has been an error in at least on client. Gathering logs...")
        Machine.gather_logs_all(machines)


def arrival_rate_test(args):
    arrival_rates = [40, 60, 80, 100] #list(range(20, 101, 20))
    read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])

    if not args.only_create:
        make_sure_project_can_be_built()
    args.no_build = True

    for ar in arrival_rates:
        workload = json.load(open("../config/auto_test/input_workload.json", "r"), object_pairs_hook=OrderedDict)["workload_config"]
        for grp_con in workload:
            groups = grp_con["grp_workload"]
            for grp in groups:
                grp["arrival_rate"] = ar

        json.dump({"workload_config": workload}, open("../config/auto_test/input_workload.json", "w"), indent=2)

        main(args)
        os.system("mv data/CAS_NOF data/arrival_rate/HR/CAS_NOF_" + str(ar))

def object_number_test(args):
    object_number = [1, 5, 25, 125]
    # read_ratios = OrderedDict([("HW", 0.1)]) #, ("RW", 0.5), ("HR", 0.9)])
    read_ratios = OrderedDict([("RW", 0.5), ("HR", 0.9)])

    if not args.only_create:
        make_sure_project_can_be_built()
    args.no_build = True

    for rr in read_ratios:
        for on in object_number:
            workload = json.load(open("../config/auto_test/input_workload.json", "r"), object_pairs_hook=OrderedDict)["workload_config"]
            for grp_con in workload:
                groups = grp_con["grp_workload"]
                for grp in groups:
                    keys = []
                    key_id = 2000
                    for i in range(1, on + 1):
                        keys.append(str(key_id + i))
                    grp["read_ratio"] = read_ratios[rr]
                    grp["write_ratio"] = 1 - read_ratios[rr]
                    grp["keys"] = keys

            json.dump({"workload_config": workload}, open("../config/auto_test/input_workload.json", "w"), indent=2)

            main(args)
            os.system("mv data/CAS_NOF data/object_number/" + rr + "/CAS_NOF_" + str(on))

if __name__ == '__main__':
    args = parse_args()
    # print(can_project_be_built())

    main(args)
    # arrival_rate_test(args)
    # object_number_test(args)

    # print("Main thread goes to sleep")
    # threading.Event().wait()
