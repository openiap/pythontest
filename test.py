import json
from openiap import Client, ClientError
import time, os



# Main function
if __name__ == "__main__":
    client = Client()
    try:
        # client.enable_tracing("openiap=trace", "new")
        client.enable_tracing("openiap=info", "")
        client.info("Connecting to OpenIAP")
        client.connect()

        eventcounter = [0]  
        def onclientevent(result, counter):
            eventcounter[0] += 1
            event = result["event"]
            reason = result["reason"]
            client.info(f"Client event #{counter} Received {result} event: {reason}")

        eventid = client.on_client_event(callback=onclientevent)
        client.info("Client event, registered with id: ", eventid)
        signin_result = client.signin()
        client.info(signin_result)

        # # for x in range(1, 10):
        # #     client.query(collectionname="entities", query="{}", projection="{\"name\": 1}", orderby="", queryas="", explain=False, skip=0, top=0)


        # client.info("Turning off client event, id: ", eventid)
        # client.off_client_event(eventid)


        collections = client.list_collections()
        client.info(collections)

        client.create_collection("python_testcol")
        insert_one_result = client.insert_one(collectionname="python_testcol", item="{\"name\": \"test from python\", \"_type\": \"test\"}")

        # client.create_collection("python_testcol", timeseries=(

        client.drop_collection("python_testcol")

        files = []
        if(os.path.exists("testfile.csv")):
            files.append("testfile.csv")
        else:
            files.append("../testfile.csv")
        workitem = client.push_workitem( name="python test with file", wiq="rustqueue", payload="{}", files=files)
        client.info(workitem)

        workitem = client.pop_workitem( wiq="rustqueue")
        client.info(workitem)
        workitem["state"] = "successful"
        client.update_workitem(workitem)
        client.info(workitem)

        client.delete_workitem(workitem["id"])

        workitem = client.push_workitem( name="python without file", wiq="rustqueue", payload="{}")
        client.info(workitem)

        workitem = client.pop_workitem( wiq="rustqueue")
        client.info(workitem)
        workitem["state"] = "successful"
        workitem["name"] = "python updated, now including a file"
        files = []
        if(os.path.exists("testfile.csv")):
            files.append("testfile.csv")
        else:
            files.append("../testfile.csv")

        client.update_workitem(workitem, files)
        client.info(workitem)
        client.delete_workitem(workitem["id"])

        files = []
        workitem = client.push_workitem( name="python without file", wiq="rustqueue", payload="{}")
        client.info(workitem)
        workitem = client.pop_workitem( wiq="rustqueue")
        workitem["state"] = "successful"
        client.update_workitem(workitem, files)


        query_result = client.query(collectionname="entities", query="{}", projection="{\"name\": 1}", orderby="", queryas="", explain=False, skip=0, top=0)
        client.info(query_result)

        aggregate_result = client.aggregate(collectionname="entities", aggregates="[]")
        client.info(aggregate_result)
        
        insert_one_result = client.insert_one(collectionname="entities", item="{\"name\": \"test from python\", \"_type\": \"test\"}")
        client.info(insert_one_result)
        item = json.loads(insert_one_result)
        item["name"] = "test from python updated"
        update_one_result = client.update_one(collectionname="entities", item=json.dumps(item))
        client.info(update_one_result)
        id = item["_id"]

        insert_one_result = client.insert_one(collectionname="entities", item="{\"name\": \"test from python\", \"_type\": \"test\"}")
        client.info(insert_one_result)
        item = json.loads(insert_one_result)
        id2 = item["_id"]


        delete_many_query = client.delete_many(collectionname="entities", ids=[id, id2])
        client.info("Deleted ", delete_many_query, " items using ids")

        client.insert_many(collectionname="entities", items="[ {\"name\": \"test from python updated\", \"_type\": \"test\"}, {\"name\": \"test from python updated\", \"_type\": \"test\"} ]")
        client.info("added 2 items")

        delete_many_query = client.delete_many(collectionname="entities", query="{\"name\": \"test from python updated\"}")
        client.info("Deleted ", delete_many_query, " items using query")
        
        insert_or_update_one_result = client.insert_or_update_one(collectionname="entities", item="{\"name\": \"test insert or update from python\", \"_type\": \"test\"}")
        client.info(insert_or_update_one_result)
        item = json.loads(insert_or_update_one_result)

        client.delete_one(collectionname="entities", id=item["_id"])

        download_result = client.download(collectionname="fs.files", id="65a3aaf66d52b8c15131aebd", folder="", filename="")
        client.info(download_result)

        filepath = "testfile.csv"
        # file exists ?
        if not os.path.exists(filepath):
            filepath = "../" + filepath
        upload_result = client.upload(filepath=filepath, filename="python-test.csv", mimetype="", metadata="", collectionname="fs.files")

        watchcounter = [0]  
        def onwatch(event, counter):
            watchcounter[0] += 1
            operation = event["operation"]
            # client.info(f"{counter} Received event: {json.dumps(event, indent=2)}")
            client.info(f"{counter} Received {operation} event: ")

        watch_result = client.watch(collectionname="entities", paths="", callback=onwatch)
        client.info(watch_result)
        client.insert_many(collectionname="entities", items="[ {\"name\": \"watch test from python 1\", \"_type\": \"test\"}, {\"name\": \"watch test from python 2\", \"_type\": \"test\"} ]")

        while watchcounter[0] < 2:
            time.sleep(1)
        unwatch_result =  client.unwatch(watch_result)
        client.info(unwatch_result)

        count_result = client.count(collectionname="entities", query="{}")
        client.info(count_result)

        distinct_result = client.distinct(collectionname="entities", field="_type", query="{}")
        client.info(distinct_result)

        queuecounter = [0]  
        def onmessage(event, counter):
            queuecounter[0] += 1
            data = event["data"]
            # client.info(f"{counter} Received event: {json.dumps(event, indent=2)}")
            client.info(f"{counter} Received {data} event: ")

        register_queue_result = client.register_queue(queuename="test2queue", callback=onmessage)
        client.info(register_queue_result)

        client.queue_message(queuename="test2queue", data="{\"test\": \"message 1\"}", striptoken=True)
        client.queue_message(queuename="test2queue", data="{\"test\": \"message 2\"}", striptoken=True)

        while queuecounter[0] < 2:
            time.sleep(1)
        unregister_queue = client.unregister_queue(register_queue_result)
        client.info(unregister_queue)


        exchangecounter = [0]  
        def onexchange(event, counter):
            exchangecounter[0] += 1
            data = event["data"]
            # client.info(f"{counter} Received event: {json.dumps(event, indent=2)}")
            client.info(f"{counter} Received {data} event: ")

        register_exchange_result = client.register_exchange(exchangename="testexc", callback=onexchange)
        client.info(register_exchange_result)

        client.queue_message(exchangename="testexc", data="{\"test\": \"message 1\"}", striptoken=True)
        client.queue_message(exchangename="testexc", data="{\"test\": \"message 2\"}", striptoken=True)

        while exchangecounter[0] < 2:
            time.sleep(1)
        unregister_queue =  client.unregister_queue(register_exchange_result)
        client.info(unregister_queue)

    except ClientError as e:
        client.error(f"An error occurred: {e}")
    print("*********************************")
    print("done, free client")
    print("*********************************")
    client.free()
