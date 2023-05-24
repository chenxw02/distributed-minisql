import threading
import socket
import random

from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
#master_name = "minisql1"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

zk.ensure_path("/servers/minisql1/tables")
zk.ensure_path("/servers/minisql2/tables")
zk.ensure_path("/servers/minisql3/tables")
zk.ensure_path("/clients/minisql1/done")
zk.ensure_path("/clients/minisql2/done")
zk.ensure_path("/clients/minisql3/done")
zk.ensure_path("/tables")




#测试所要加的

zk.ensure_path("/tables/test_table/server1")
zk.ensure_path("/tables/test_table/server2")
zk.ensure_path("/servers/minisql1/tables/test_table")
zk.ensure_path("/servers/minisql2/tables/test_table")
zk.set("/tables/test_table/server1", "minisql1".encode())
zk.set("/tables/test_table/server2", "minisql2".encode())
if zk.exists("/servers/minisql3/tables/test_table"):
    zk.delete("/servers/minisql1/tables/test_table")



