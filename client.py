from kazoo.client import KazooClient
import random
import time
from functools import partial

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

servers = ["minisql1", "minisql2", "minisql3"]

completed_servers = []

def notify_done(server, data, stat, event):
    if event is not None and (event.type == "CREATED" or event.type == "CHANGED"):
        print("Master has finished processing: %s" % data.decode("utf-8"))
        completed_servers.append(server)
        print(server)
        if zk.exists("/clients/" + server + "/done"):
            zk.delete("/clients/" + server + "/done")

for server in servers:
    zk.DataWatch("/clients/" + server + "/done", partial(notify_done, server))

while True:
    # Get user input
    instruction = input("Please enter your instruction: ")

    # Pick two random servers
    selected_servers = random.sample(servers, 2)

    for server in selected_servers:
        print("Sending instruction to %s" % server)

        # Make sure the parent node exists
        if not zk.exists("/servers/" + server):
            zk.create("/servers/" + server)

        # Delete the old instruction if it exists
        if zk.exists("/servers/" + server + "/instructions"):
            zk.delete("/servers/" + server + "/instructions")

        # Set the instruction and watch for a done signal
        zk.create("/servers/" + server + "/instructions/", instruction.encode("utf-8"), ephemeral=True)
        data, stat = zk.get("/servers/" + server + "/instructions")

        # zk.DataWatch("/clients/" + server + "/done", notify_done)

    # Wait for both masters to process the instruction
    while len(completed_servers) < 2:
        time.sleep(1)

    # Reset the completed servers list for the next round
    completed_servers = []
