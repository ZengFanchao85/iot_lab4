# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "./data/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "./data/car_{}/certificate.pem.crt"
key_formatter = "./data/car_{}/private.pem.key"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a2unjb3o1vi1xt-ats.iot.ap-southeast-1.amazonaws.com", 8883)
        self.client.configureCredentials("data/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass

    def subscribe(self):
        self.client.subscribeAsync(f"vehicle/emission/data/veh{self.device_id}/maxco2", 0, ackCallback=self.customSubackCallback)
        print(f'subscribe to vehicle/emission/data/veh{self.device_id}/maxco2')


    def publish(self, topic="vehicle/emission/data"):
        #TODO4: fill in this function for your publish
        #self.client.subscribeAsync("vehicle/emission/data/car_{self.device_id}", 0, ackCallback=self.customSubackCallback)
        #self.client.publishAsync("vehicle/emission", Payload, 0, ackCallback=self.customPubackCallback)
        # Load the vehicle's emission data
        df = pd.read_csv(data_path.format(self.device_id))
        for index, row in df.iterrows():
            # Create a JSON payload from the row data
            payload = json.dumps(row.to_dict())
            
            # Publish the payload to the specified topic
            print(f"Publishing: {payload} to {topic}")
            self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)
            # Sleep to simulate real-time data publishing

print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    client.client.connect()
    clients.append(client)

while True:
    print("send now?")
    x = input()
    if x == "s":
        for i,c in enumerate(clients):
            c.publish()

    elif x =='a':
        for i,c in enumerate(clients):
            c.subscribe()

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)
