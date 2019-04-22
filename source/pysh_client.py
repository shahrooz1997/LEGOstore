import getpass
import random
import string
from random import randint
import platform, subprocess
from threading import Thread
import os
import datetime
import sys
import sys
import time
import logging
from time import sleep
from functools import partial

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler

#A simple Client that send messages to the echo server
from twisted.internet import reactor, protocol

textevent = ""



class EchoClient(protocol.Protocol):
    def connectionMade(self):
        self.factory.app.on_connection(self.transport)

    def dataReceived(self, data):
        self.factory.app.print_message(data)

class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient
    def __init__(self, app):
        self.app = app

    def clientConnectionLost(self, conn, reason):
        self.app.print_message("connection lost")

    def clientConnectionFailed(self, conn, reason):
        self.app.print_message("connection failed")



# A simple kivy App, with a textbox to enter messages, and
# a large label to display all the messages received from
# the server
class TwistedClientApp():
    def __init__(self):
        self.connection = None
        self.user_os = None
        self.user = None
        self.labelmsg = ""
        self.chat_online_details = ""
        self.haha = []
        self.y=0
        self.notify_events = []
        self.available_users = []
        self.setup_gui()

    def setup_gui(self):
        global user
        user = getpass.getuser()
        global user_os
        user_os = platform.system()
        self.dir_loc = "/home/ubuntu/"
        self.ctexthost = "18.220.45.147"
        self.ctextport = 9090

    def building_utilities(self, *args):
        root_dirname = self.reponame.text
        if not os.path.exists(root_dirname):
            os.makedirs(root_dirname)
        os.chdir(root_dirname)
        self.dir_loc = os.getcwd()
    def connect_start(self,*args):
        self.conn = reactor.connectTCP(str(self.ctexthost), int(self.ctextport), EchoFactory(self))
        self.start_thread()
        reactor.run()

    def on_connection(self, connection):
        self.print_message("Connected Succesfully!")
        self.connection = connection
        #self.repo_creation()
    def send_message(self,connection):
        msg = "HI"
        if msg and self.connection:
            msg = "2$#" + user + "@" + user_os + ": " + msg
            self.connection.write(str(msg))

    def print_message(self, msg):
        print (msg)

    def CloseClient(self, *args):
        self.conn.disconnect()
        path = os.path.dirname(os.getcwd())
        os.chdir(path)
        print (os.getcwd())

    def stimulate_on_repo_changes(self):
        while True:
            #self.logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
            self.path = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
            self.event_handler = ChangeHandler(self)
            self.observer = Observer()
            self.observer.schedule(self.event_handler, self.path, recursive=True)
            self.observer.start()
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.observer.stop()
                self.observer.join()

    def start_thread(self):
        t = Thread(target=self.do_job)
        t.start()

    def notify(self,textevent):
        textevent1 = "4$#" + user + "@" + user_os + ": " + str(textevent)
        #textevent = "4$#" + textevent
        self.connection.write(str(textevent1).encode('utf-8'))
    def random_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))
    def do_job(self):
        sleep(2)
        print ("doing job...")
        file_names = ["key{}".format(i) for i in range(100)]

        for i in range(3000):
            #print ("=========================Job number {}================\n".format(i+1))
            file_name = file_names[randint(0,99)]
            op = randint(0,1)
            if op:
                self.notify(file_name+":read:")
            else:
                content = self.random_generator(1024)
                self.notify(file_name+":write:"+content)
                #read operation





if __name__ == '__main__':
    t =  TwistedClientApp()
    t.connect_start()
