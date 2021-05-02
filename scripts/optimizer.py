#!/usr/bin/python3

import command
import subprocess
from subprocess import PIPE
import threading
import os, shutil
import sys, time
from time import sleep
from signal import signal, SIGINT
import urllib.request
import argparse
import run_optimizer as optimizer
from collections import OrderedDict
import json
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))

lat_sense = False
cold_hot = False
coldhot_executions = OrderedDict([("optimized", "-H min_cost"), ("baseline_cas", "-H min_cost -b -t cas -m 4 -k 2")])

if lat_sense:
    from optimizer.Experiments_Lat_sense.workload_def import *
else:
    from optimizer.Experiments.workload_def import *
from random import shuffle

server_type = "e2-standard-4"
num_of_servers = 8


def get_workloads(availability_target):
    f_index = availability_targets.index(availability_target)

    workloads = []
    for client_dist in client_dists:
        for object_size in object_sizes:
            for storage_size in storage_sizes:
                for arrival_rate in arrival_rates:
                    for read_ratio in read_ratios:
                        if lat_sense:
                            for lat in SLO_latencies:
                                # lat = SLO_latencies[f_index]

                                FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                                    arrival_rate) + "_" + read_ratio + "_" + str(lat) + ".json"
                                workloads.append(FILE_NAME)
                        else:
                            # lat = SLO_latencies[f_index]
                            #
                            # FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                            #     arrival_rate) + "_" + read_ratio + "_" + str(lat) + ".json"
                            # workloads.append(FILE_NAME)

                            # for lat in SLO_latencies:
                            lat = SLO_latencies[f_index]

                            FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                                arrival_rate) + "_" + read_ratio + "_" + str(lat) + ".json"
                            workloads.append(FILE_NAME)

    return workloads

def get_commands():
    commands = []
    directory = "workloads"

    if cold_hot:
        for cold_ratio in range(0, 101, 1):
            cold_ratio_p = cold_ratio / 100.
            file_name = "workload_" + str(cold_ratio) + ".json"

            files_path = "coldhot/workloads/cold"
            for exec in coldhot_executions:
                result_file_name = "res_" + exec + "_" + file_name
                commands.append(
                    "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                    file_name) + " -o " + os.path.join(
                        files_path, result_file_name) + " -v " + coldhot_executions[exec])

        exec = "baseline_cas"
        for cold_ratio in range(0, 101, 1):
            cold_ratio_p = cold_ratio / 100.
            file_name = "workload_" + str(cold_ratio) + ".json"

            files_path = "coldhot/workloads/hot"
            result_file_name = "res_" + exec + "_" + file_name
            commands.append(
                "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                file_name) + " -o " + os.path.join(
                    files_path, result_file_name) + " -v " + coldhot_executions[exec])


        shuffle(commands)
        return commands

    # print(workload_files)
    for f_index, f in enumerate(availability_targets):
        workload_files = get_workloads(f)
        for file_name in workload_files:
            for exec in executions[f_index]:
                files_path = os.path.join(directory, "f=" + str(f))
                result_file_name = "res_" + exec + "_" + file_name
                commands.append(
                    "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                    file_name) + " -o " + os.path.join(
                        files_path, result_file_name) + " -v " + executions[f_index][exec])


    # for f_index, f in enumerate(availability_targets):
    #     files_path = os.path.join(directory, "f=" + str(f))
    #     for file_name in workload_files:
    #         for exec in list(executions[0].keys()):
    #             result_file_name = "res_" + exec + "_" + file_name
    #             commands.append("python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
    #                                                                                                   file_name) + " -o " + os.path.join(
    #                 files_path, result_file_name) + " -H min_cost -v " + executions[f_index][exec])
    shuffle(commands)
    return commands

def get_commands_k(const_m=False):
    commands = []
    directory = "workloads"
    m = 9

    # print(workload_files)
    for f_index, f in enumerate(availability_targets):
        workload_files = get_workloads(f)
        for file_name in workload_files:
            for k in range(1, m-1, 1):
                files_path = os.path.join(directory, "f=" + str(f))
                result_file_name = "res_" + str(k) + "_" + file_name
                if const_m:
                    commands.append(
                        "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                        file_name) + " -o " + os.path.join(
                            files_path, result_file_name) + " -v -H min_cost -b -t cas -m " + str(m) + " -k " + str(k))
                else:
                    commands.append(
                        "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                        file_name) + " -o " + os.path.join(
                            files_path, result_file_name) + " -v -H min_cost -b -t cas -m " + str(k+2*f) + " -k " + str(k))

    shuffle(commands)
    return commands

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
def read_zones():
    zones = ["us-central1-b" for _ in range(num_of_servers)]
    return zones

files_lock = threading.Lock()
def create_project_tar_file():
    files_lock.acquire()
    if lat_sense:
        os.system("./copy_optimizer2.sh")
    else:
        os.system("./copy_optimizer.sh")

def delete_project_tar_file():
    os.system("rm -rf optimizer.tar.gz optimizer")
    files_lock.release()

class Machine:

    existing_info = ''
    existing_info_lock = threading.Lock()

    def get_machine_list():
        def create_machine_thread_helper(machines, m_name, m_type, m_zone):
            machines[m_name] = Machine(name=m_name, type=m_type, zone=m_zone)
        zones = read_zones()
        threads = []
        servers = OrderedDict()
        for i, zone in enumerate(zones):
            name = "worker" + str(i)
            threads.append(threading.Thread(target=create_machine_thread_helper, args=[servers, name, server_type, zone]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        servers = list(servers.items())
        servers.sort(key=lambda x: x[0])
        servers = OrderedDict(servers)

        print("All machines are ready to use")
        return servers

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

    def run_all(servers, commands, clear_all=True): # if you call this method, you should not call run method anymore. It does not make sense to run twice!
        create_project_tar_file()
        os.system("mkdir -p /tmp/optimizer_autorun/")
        threads = []

        indicies = []
        batch_size = int(len(commands) / num_of_servers)
        indicies.append((0, batch_size))
        for i in range(1, num_of_servers - 1):
            indicies.append((i * batch_size + 1, (i + 1) * batch_size))
        indicies.append(((num_of_servers - 1) * batch_size + 1, len(commands) - 1))

        server_index = 0
        for name, server in servers.items():
            threads.append(threading.Thread(target=server.run, args=[commands[indicies[server_index][0]:indicies[server_index][1] + 1], clear_all]))
            threads[-1].start()
            server_index += 1

        for thread in threads:
            thread.join()

        delete_project_tar_file()
        os.system("rm -rf /tmp/optimizer_autorun/")

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

    def gather_results_all(machines, run_name):
        threads = []
        os.system("mkdir -p data/" + run_name + "_optimizer/")
        for name, client in machines.items():
            threads.append(threading.Thread(target=client.gather_results, args=[run_name]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_logs_all(machines, run_name, type=None):
        threads = []
        os.system("mkdir -p data/" + run_name + "_optimizer/")
        for name, client in machines.items():
            threads.append(threading.Thread(target=client.gather_logs, args=[run_name, type]))
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
                "sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git psmisc bc libgflags-dev cmake >/dev/null 2>&1")
            self.execute("sudo touch ../init_config_done")

    def config(self, clear_all=True):
        # self.stop()
        if clear_all:
            self.clear()
        print("sending the optimizer to machine " + self.name)
        project_tar_file_created = False
        if not os.path.exists("optimizer.tar.gz"):
            create_project_tar_file()
            project_tar_file_created = True
        self.copy_to("optimizer.tar.gz", "")
        if project_tar_file_created:
            delete_project_tar_file()

        self.execute("tar -xzf optimizer.tar.gz")

    def get_latencies(self, machines):
        self.copy_to("./myping", "")
        command = "./myping " + self.name[1:] + " "
        for name, machine in machines.items():
            command += machine.external_ip + " "

        command = command[:-1]
        self.execute(command)
        os.system("rm -f pairwise_latencies/latencies_from_server_" + self.name[1:] + ".txt")
        self.copy_from("latencies_from_server_" + self.name[1:] + ".txt", "pairwise_latencies")

    def clear(self):
        self.execute("rm -rf *")

    def make_clear(self):
        self.execute("cd project/; sudo make cleanall")

    def delete(self):
        command.delete_server(self.name, self.zone)

    def run(self, commands, clear_all=True):
        self.stop()
        self.config(clear_all)

        commands_str = ""
        for command in commands:
            commands_str += command + '\n'
        # print("cd optimizer/Experiments; echo \"" + commands_str + "\" > commands.txt")
        os.system("mkdir -p /tmp/optimizer_autorun/")
        f = open("/tmp/optimizer_autorun/" + "commnads_" + self.name + ".txt", "w")
        f.write(commands_str)
        f.close()
        if lat_sense:
            self.copy_to("/tmp/optimizer_autorun/" + "commnads_" + self.name + ".txt", "optimizer/Experiments_Lat_sense/commands.txt")
        else:
            self.copy_to("/tmp/optimizer_autorun/" + "commnads_" + self.name + ".txt", "optimizer/Experiments/commands.txt")
        print("execution started on " + self.name)
        start_time = time.time()
        if lat_sense:
            self.execute("cd optimizer/Experiments_Lat_sense; ./command_runner.py >command_runner_output_" + self.name + ".txt 2>&1")
        else:
            self.execute("cd optimizer/Experiments; ./command_runner.py >command_runner_output_" + self.name + ".txt 2>&1")

        print("execution finished on " + self.name + " in " + str(time.time() - start_time) + "s.")

    def stop(self):
        #Todo: kill python3 ./placement
        self.execute("sudo pkill -f command_runner.py")
        self.execute("sudo pkill -f \"python3 ../placement.py\"")
        # self.stop_server()
        # self.stop_metadata_server()

    def gather_results(self, run_name):
        if cold_hot:
            os.system("mkdir -p data/" + run_name + "_optimizer/cold")
            stdout, stderr = self.execute("ls optimizer/Experiments/coldhot/workloads/cold" + "/res_*.json")
            if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
                self.copy_from("optimizer/Experiments/coldhot/workloads/cold" + "/res_*.json",
                               "data/" + run_name + "_optimizer/cold")

            os.system("mkdir -p data/" + run_name + "_optimizer/hot")
            stdout, stderr = self.execute("ls optimizer/Experiments/coldhot/workloads/hot" + "/res_*.json")
            if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
                self.copy_from("optimizer/Experiments/coldhot/workloads/hot" + "/res_*.json",
                               "data/" + run_name + "_optimizer/hot")
            return

        for f in availability_targets:
            os.system("mkdir -p data/" + run_name + "_optimizer/f=" + str(f))
            if lat_sense:
                stdout, stderr = self.execute("ls optimizer/Experiments_Lat_sense/workloads/f=" + str(f) + "/res_*.json")
            else:
                stdout, stderr = self.execute("ls optimizer/Experiments/workloads/f=" + str(f) + "/res_*.json")
            if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
                if lat_sense:
                    self.copy_from("optimizer/Experiments_Lat_sense/workloads/f=" + str(f) + "/res_*.json", "data/" + run_name + "_optimizer/f=" + str(f))
                else:
                    self.copy_from("optimizer/Experiments/workloads/f=" + str(f) + "/res_*.json", "data/" + run_name + "_optimizer/f=" + str(f))

    def gather_logs(self, run_name, type=None):
        if type is not None:
            run_name += "_" + type
        for f in availability_targets:
            os.system("mkdir -p data/" + run_name + "_optimizer/f=" + str(f))
            if lat_sense:
                stdout, stderr = self.execute("ls optimizer/Experiments_Lat_sense/workloads/f=" + str(f) + "/res_*_output.txt")
            else:
                stdout, stderr = self.execute("ls optimizer/Experiments/workloads/f=" + str(f) + "/res_*_output.txt")
            if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
                if lat_sense:
                    self.copy_from("optimizer/Experiments_Lat_sense/workloads/f=" + str(f) + "/res_*_output.txt", "data/" + run_name + "_optimizer/f=" + str(f))
                else:
                    self.copy_from("optimizer/Experiments/workloads/f=" + str(f) + "/res_*_output.txt", "data/" + run_name + "_optimizer/f=" + str(f))
        if lat_sense:
            self.copy_from("optimizer/Experiments_Lat_sense/command_runner_output_" + self.name + ".txt", "data/" + run_name + "_optimizer/")
        else:
            self.copy_from("optimizer/Experiments/command_runner_output_" + self.name + ".txt", "data/" + run_name + "_optimizer/")

def get_commands_with_opt_m_k():
    commands = get_commands()
    ret_commands = []

    files_path = "../optimizer/Experiments/workloads"

    for command in commands:
        exclude = False
        if command.find("nearest") == -1:
            continue
        workload = command[command.find("workloads") + 10:command.find(".json",command.find("workloads"))] + ".json"
        f = workload[2]
        workload = workload[4:]
        # print(workload)

        if command.find("cas") != -1:
            res_file = os.path.join(files_path, "f=" + f, "res_" + "baseline_cas" + "_" + workload)
            data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
            for grp_index, grp in enumerate(data):
                m = grp["m"]
                if m == "INVALID":
                    exclude = True
                    break
                k = grp["k"]
            if not exclude:
                new_command = command[:command.find("-m ") + 3] + str(m) + command[command.find("-m ") + 4:command.find("-k ") + 3] + str(k) + command[command.find("-k ") + 5:]
        else:
            res_file = os.path.join(files_path, "f=" + f, "res_" + "baseline_abd" + "_" + workload)
            data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
            for grp_index, grp in enumerate(data):
                m = grp["m"]
                if m == "INVALID":
                    exclude = True
                    break
            if not exclude:
                new_command = command[:command.find("-m ") + 3] + str(m) + command[command.find("-m ") + 5:]
        # print(new_command)
        if not exclude:
            ret_commands.append(new_command)

    return ret_commands

def commands_exclude(commands, word):
    new_commands = []
    for command in commands:
        if command.find(word) != -1:
            continue
        new_commands.append(command)

    return new_commands

def execute(machines, commands, type=None):
    Machine.run_all(machines, commands)
    print("Project execution finished.")
    print("Please wait while I am stopping all the machines and gathering the results...")
    os.system("rm -rf /home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/ALL_optimizer")
    Machine.gather_results_all(machines, "ALL")

    if not cold_hot:
        Machine.gather_logs_all(machines, "ALL", type)

    # Copy files to workloads
    if lat_sense:
        os.system("cp -rf data/ALL_optimizer/* ../optimizer/Experiments_Lat_sense/workloads/")
    else:
        if cold_hot:
            os.system("cp -rf data/ALL_optimizer/* ../optimizer/Experiments/coldhot/workloads/")
        else:
            os.system("cp -rf data/ALL_optimizer/* ../optimizer/Experiments/workloads/")

def main(args):
    machines = Machine.get_machine_list()

    if lat_sense:
        os.system("cd ../optimizer/Experiments_Lat_sense; ./generate_input.py")
    else:
        if cold_hot:
            os.system("cd ../optimizer/Experiments/coldhot; ./generate_input.py")
        else:
            os.system("cd ../optimizer/Experiments; ./generate_input.py")

    # uncomment one line of the following two lines
    commands = get_commands()
    commands = commands_exclude(commands, "nearest")
    # commands = get_commands_k()

    execute(machines, commands)

    # if not lat_sense and not cold_hot:
    #     commands = get_commands_with_opt_m_k()
    #     execute(machines, commands, "nearest")

    # delete the machines
    # os.system("./delete_servers.py")

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
