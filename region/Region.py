import sys
import os
import socket
import logging

from kazoo.client import KazooClient

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from configs.config import zookeeperConfig
from API import *
from buffer_manager import Buffer
from buffer_manager import Block
from interpreter import *


class region:

    def __init__(self):

        self.master_ip = '127.0.0.1'
        self.master_port = 8888
        self.server_name = sys.argv[1]
        self.c = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #socket
        self.buf = Buffer()

        hosts = zookeeperConfig['hosts']
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        zk = KazooClient(hosts=hosts, logger=logging)
        zk.start()

        self.connect_Master()

        party = zk.Party('/party', self.server_name)
        party.join()

    def connect_Master(self):
        if self.c.connect((self.master_ip, self.master_port)) == socket.error:
            print('Connection failed')
        data = self.server_name.encode()
        self.c.send(data)
        result = self.c.recv(1024).decode()
        print(result)

    def listening(self):
        # 等待数据库操作指令
        while 1:
            instruction = self.c.recv(1024).decode()
            print(instruction)
            ret = interpret(instruction, buf=self.buf)
            if instruction.find("select") != -1:
                self.c.send(self.reform(ret))
            print("done")

    def reform(self, input_tuple):
        data, headers = input_tuple
        result = ','.join(headers) + '\n'
        for row in data:
            row_str = ','.join(str(value) for value in row)
            result += row_str + '\n'
        result += 'send end'
        return result


r = region()
r.listening()

