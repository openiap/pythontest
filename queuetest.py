from openiap import Client
import json
import time
def onmessage(event, counter):
    data = event["data"]
    print(f"{counter} Received event: {json.dumps(event, indent=2)}")
    workitem = client.pop_workitem(queuename="testq")
    print("workitem", workitem)
    if workitem:
        print("workitem", workitem)
        workitem["state"] = "successfull"
        client.update_workitem(workitem)
    else:
        print("no workitem")

def onclientevent(result, counter):
    print("client event", result)
    event = result["event"]
    if event == "SignedIn":
        print("client signed in, registering queue")
        queuename = client.register_queue(queuename="testq", callback=onmessage)
        print("queue", queuename, "registered")

client = Client()
client.connect()
eventid = client.on_client_event(callback=onclientevent)
while True:
    time.sleep(1)