from kazoo.client import KazooClient
import random
import time

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

servers = ["minisql1", "minisql2", "minisql3"]

def notify_done(data, stat, event):
    if event is not None and event.type == "CREATED":
        print("Master has finished processing: %s" % data.decode("utf-8"))
        zk.delete("/clients/" + server + "/done")

while True:
    # Get user input
    instruction = input("Please enter your instruction: ")

    # Pick a random server
    server = random.choice(servers)
    print("Sending instruction to %s" % server)

    # 打印所有节点

    # Make sure the parent node exists
    if not zk.exists("/servers/"):
        zk.create("/servers/")

    if not zk.exists("/servers/" + server):
        zk.create("/servers/" + server)

    # Delete the old instruction if it exists
    if zk.exists("/servers/" + server + "/instructions"):
        zk.delete("/servers/" + server + "/instructions")

    # Set the instruction and watch for a done signal
    zk.create("/servers/" + server + "/instructions/", instruction.encode("utf-8"), ephemeral=True)
    data, stat = zk.get("/servers/" + server + "/instructions")
    print("Data: %s" % data.decode("utf-8"))

    zk.DataWatch("/clients/" + server + "/done", notify_done)

    # Wait for the master to process the instruction
    while zk.exists("/servers/" + server + "/instructions"):
        time.sleep(1)

