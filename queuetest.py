from openiap import Client
import time
import json
import threading

# Define the workitem queue name
WIQ_NAME = "testq"

def process_workitem(workitem):
    """Process a single workitem"""
    client.info(f"Processing workitem: {workitem['id']}")
    
    try:
        # Parse the payload
        payload = json.loads(workitem["payload"]) if workitem.get("payload") else {}
        client.info(f"Workitem payload: {payload}")
        
        # Do some processing here
        # For example:
        if "data" in payload:
            payload["result"] = f"Processed: {payload['data']}"
        
        # Update the workitem with the processed payload
        workitem["payload"] = json.dumps(payload)
        workitem["state"] = "successful"
        client.update_workitem(workitem)
        
        client.info(f"Workitem {workitem['id']} processed successfully")
    
    except Exception as e:
        client.error(f"Error processing workitem: {str(e)}")
        workitem["state"] = "retry"
        workitem["errormessage"] = str(e)
        client.update_workitem(workitem)

def loop_workitems():
    """Check for and process any available workitems"""
    client.info(f"Checking for workitems in queue '{WIQ_NAME}'")
    counter = 0
    
    while True:
        workitem = client.pop_workitem(WIQ_NAME)
        if workitem is None:
            break
        
        counter += 1
        process_workitem(workitem)
    
    if counter > 0:
        client.info(f"Processed {counter} workitems")
    else:
        client.info("No workitems found")

def handle_message(event, count):
    """Handle incoming message from the queue"""
    client.info(f"Received message #{count}: {event}")
    
    # Start processing workitems in a separate thread
    thread = threading.Thread(target=loop_workitems)
    thread.daemon = True
    thread.start()

def on_connected():
    """Called when the client is connected and signed in"""
    client.info("Connected and signed in, registering queue...")
    client.register_queue(WIQ_NAME, handle_message)
    client.info(f"Queue '{WIQ_NAME}' registered successfully")
    
    # Process any existing workitems immediately
    loop_workitems()

def handle_event(event, count):
    """Handle client events"""
    client.info(f"Event: {event['event']}")
    if event["event"] == "SignedIn":
        on_connected()

# Initialize client
client = Client()

try:
    # Connect and sign in
    client.info("Connecting to OpenIAP...")
    client.connect()
    client.info("Connected, signing in...")
    
    # Set up event handler
    client.on_client_event(handle_event)
    
    # Keep the script running
    client.info("Waiting for messages...")
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    client.info("Shutting down...")
except Exception as e:
    client.error(f"Error: {str(e)}")
finally:
    # Clean up
    client.info("Disconnecting...")
    client.disconnect()