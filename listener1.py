from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
master_name = "minisql1"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

def handle_instruction(data, stat, event):
    # Handle the instruction here...
    print("Received instruction: %s" % data.decode("utf-8"))

    # When done, notify the client
    if not zk.exists("/clients/"):
        zk.create("/clients/")
    if not zk.exists("/clients/" + master_name ):
        zk.create("/clients/" + master_name)
    zk.create("/clients/" + master_name + "/done", b"Done", ephemeral=True)
    zk.delete(event.path)

@zk.DataWatch("/servers/" + master_name + "/instructions")
def watch_node(data, stat, event):
    if event is not None and event.type == "CREATED":
        handle_instruction(data, stat, event)

while True:
    # Keep your program running or the listener will stop
    time.sleep(1)
