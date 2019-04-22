from twisted.internet import reactor
from twisted.internet import protocol
from Client import *
import json
clients = []
PyShareLogFile = open("PyShareLogFile.log","w")
import signal
import sys
def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        PyShareLogFile.close()
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
class EchoProtocol(protocol.Protocol):
    def connectionMade( self ):
        self.factory.numProtocols = self.factory.numProtocols+1
        print ("Connection from",self.transport.getPeer())
        self.temp = str(self.transport.getPeer())
        self.client_info = self.temp.split(", ")[1]
        self.client_info = self.client_info.split("'")[1]
        self.factory.addClient( self,self.client_info )

    def connectionLost( self , reason ):
        self.factory.numProtocols = self.factory.numProtocols-1
        self.temp = str(self.transport.getPeer())
        self.client_info = self.temp.split(", ")[1]
        self.client_info = self.client_info.split("'")[1]
        self.factory.delClient( self,self.client_info )

    def dataReceived(self, data):
        proc = str(data.decode('utf-8')).split("$#")[0]
        properties = json.load(open("client_config.json"))
        local_id = properties["local_datacenter"]
        cli = Client(properties, local_id)
        #print("\n ATATTA :{}\n".format((proc)), proc.isdigit())
        if (proc.isdigit() and int((proc))==2):
            response = self.factory.app.handle_message(data)
            if response:
                self.transport.write(response)
        elif (proc.isdigit() and int(str(proc))==4):
            #handle read and write first
            msg = str(data).split("$#")[1]
            file_name = msg.split(":")[1]
            op = msg.split(":")[2]
            content = ""
            #print("\n ATATA {}\n".format(op))
            if op == "write":
                print ("Got write request for file {}".format(file_name))
                content = msg.split(":")[2]
                #Do PUT to prototype
                _ , get_req_time = cli.get(file_name)

                #DO GET to protoype
                _ , put_req_time = cli.put(file_name, content)
                PyShareLogFile.write("write:" + str(get_req_time)+ ":" + str(put_req_time) + '\n')
                
                
            elif op == "read":
                print ("Got read request for file {}".format(file_name))
                #DO read to prototype and put it in the $contenti
                content, req_time = cli.get(file_name)
                PyShareLogFile.write("read:" + str(req_time)+ '\n')
                data = data.decode("utf-8") + content
                data = data.encode("utf-8")
                

            response = self.factory.sendAll(self.factory.app.handle_message(data))
        elif (proc.isdigit() and int(str(proc))==5):
            response = self.factory.app.handle_message(data)
            if response:
                self.transport.write(response)
        elif (proc.isdigit() and int(str(proc))==8):
            response = self.factory.sendTo(self,data)

class EchoFactory(protocol.Factory):
    protocol = EchoProtocol
    numProtocols = 0
    clients = []
    clientss = []

    def __init__(self, app):
        self.app = app
        #self.obj = obj
        self.logfile = open("pysharelogfile.log", "w")

    def getLogFile(self):
        return self.logfile
    def closeLogFile(self):
        self.logfile.close()
    def addClient(self, obj,newclient):
        self.clients.append( newclient )
        self.clientss.append(obj)
        #print self.clientss
    def delClient(self, obj,client):
        self.clients.remove( client )
        self.clientss.remove(obj)
        #print self.clientss
    def sendAll(self, message):
        #print self.clients
        message = message.encode("utf-8")
        for proto in self.clientss:
                proto.transport.write( message )
    def sendTo(self,obj,data):
        i = j = 0
        pos = 0
        #print (data)
        data = data.replace(" ","")
        d1 = data.split("8$#")[1]
        #print (d1)
        d2 = d1.split("##")[0]
        #print (d2)
        forward_msg_to = []
        d3 = d1.split("##")[1]
        for scan_ip in self.clients:
            print (scan_ip)
            if (d2==scan_ip):
                pos = i
            i = i + 1
        print (pos)
        for search in self.clientss:
            if (j==pos):
                forward_msg_to.append(search)
            j = j + 1
        print (search)
        for send_to in forward_msg_to:
            send_to.transport.write(data)



import subprocess
import os, platform
PORT = 9090
class TwistedServerApp():
    online_c = 0

    def start_server(self):
        print ("starting server....")
        reactor.listenTCP(int(PORT), EchoFactory(self))
        print ("Listening...")
        reactor.run()


    def on_connection(self,connection):
        self.print_message("New Client")

    def handle_message(self, msg):
        opr = str(msg.decode("utf-8")).split("$#")[0]
        msg = str(msg.decode("utf-8")).split("$#")[1]
        print (msg)
        print ("received: Event\n")
        splitmsg = msg.split(":")[0]
        curos = platform.system()
        if (opr.isdigit()):
            if (int(opr)==2):
                print ("received:  %s\n" % msg)
                msg = msg.split(": ")[1]
        root_dirname = "Hellosnp"
        if (int(opr)==2):
            if (curos=='Linux'):
                output = subprocess.Popen(["find","/home/","-name",msg+"*"],stdout=subprocess.PIPE).communicate()[0]
                msg = opr + "$#"  + output
            if (curos=='Windows'):

                output = subprocess.Popen(["dir","/s","/a","/b",msg+"*"],shell=True,stdout=subprocess.PIPE).communicate()[0]
                msg = opr + "$#" + output
                #print msg
        if (int(opr)==4):
            msg = opr + "$#" + msg
        if (int(opr)==5):
            msg = opr + "$#" + str(self.cl_conn.text)
        if (int(opr)==8):
            msg = opr + "$#" + "lol"
            print  ("received:  %s\n" % msg)
        return msg



if __name__ == '__main__':

    TwistedServerApp().start_server()