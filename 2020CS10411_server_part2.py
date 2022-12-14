import socket
import sys
import os
import hashlib
import threading
import time
import sys
import math
import collections



#basic details
localIP = "127.0.0.1"
bufferSize = 1500
noc = 5
inputFile = "A2_small_file.txt"
lock = threading.Lock()

#cache implementation
class LRUCache:
    def __init__(self, size):
        self.size = size
        self.cache = collections.OrderedDict()
    
    def get(self,packet_number):
        if packet_number not in self.cache:
            return -1
        else:
            packet = self.cache.pop(packet_number)
            self.cache[packet_number] = packet
            return packet

    def put(self,packet_number,packet):
        if packet_number not in self.cache:
            if len(self.cache)>=self.size:
                self.cache.popitem(last = False)
        else:
            self.cache.pop(packet_number)
        self.cache[packet_number] = packet

#cache of size of number of clients
cache_server = LRUCache(noc)



##Sockets declared
UDPServer_sockets = []
for i in range(noc):
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPServerSocket.bind((localIP, 20000+i))
    UDPServer_sockets.append(UDPServerSocket)

TCP_req_listner_sockets = []
TCP_req_listning_connections = []
for i in range(noc):
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, 22000+i))
    TCPServerSocket.listen(1)
    TCP_req_listner_sockets.append(TCPServerSocket)

for i in range(noc):
    TCPServerSocket = TCP_req_listner_sockets[i]
    connectionSocket, addr = TCPServerSocket.accept()  
    TCP_req_listning_connections.append(connectionSocket)

data_after_broadcast_sockets = []
for i in range(noc):
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPServerSocket.settimeout(1)
    UDPServerSocket.bind((localIP, 25000+i))
    data_after_broadcast_sockets.append(UDPServerSocket)


UDP_sender_clients = []
for i in range(noc):
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPClientSocket.settimeout(1)
    UDPClientSocket.bind(("127.0.0.1",33000+i))
    UDP_sender_clients.append(UDPClientSocket)

TCP_broadcast_clients = []
for i in range(noc):
    TCPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPClientSocket.bind(("127.0.0.1",34000+i))
    TCPClientSocket.connect(("127.0.0.1",24000+i))        
    TCP_broadcast_clients.append(TCPClientSocket)


###########################################################################################


## breaking the file in 1 kb chunks to distribute to the clients
m = open(inputFile, "r").read()
y = math.ceil(len(m)/1024)
# print(y)
client_packet_index = []
for i in range(0,noc):
    client_packet_index.append([f'{y:06d}'])
for i in range(0,noc):
    if i<y%noc:
        client_packet_index[i].append((y//noc)+1)
    else:
        client_packet_index[i].append(y//noc)
counter = 0
index = 0
# packet_data = [] 
while index<y :
    data = m[index*1024:(index+1)*1024]
    data =  f'{index:06d}'+data
    client_packet_index[counter].append(data)
    counter = (counter+1)%noc
    index += 1

TCP_connection_status = [0 for i in range(noc)]

logfile = "logfile_server.txt"
logstream = open(logfile,"a")
logstream.truncate(0)



#udp server to send chunks
def send_chunk(threadNumber):
    UDPServerSocket = UDPServer_sockets[threadNumber]
    for i in range(len(client_packet_index[threadNumber])):
        msgFromServer = client_packet_index[threadNumber][i]
        bytesToSend   = str.encode(str(msgFromServer))
        UDPServerSocket.sendto(bytesToSend,("127.0.0.1",30000+threadNumber))
        bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
        msg_received = (bytesAddressPair[0]).decode()




def listen_tcp_request(threadNumber):
    connectionSocket = TCP_req_listning_connections[threadNumber]
    while True:              
        message = connectionSocket.recv(bufferSize).decode() 
        message = int(message)        
        if message != -1:
            logstream.write("Request for packet "+str(message)+" received"+'\n')
            send_data_udp(threadNumber,message)
        else: 
            logstream.write("connection closed with "+str(addr)+'\n')
            connectionSocket.close()
            break

def send_data_udp(threadNumber,packet_number):
    UDPClientSocket = UDP_sender_clients[threadNumber]
    serverAddressPort = ("127.0.0.1", 23000+threadNumber)
    lock.acquire()
    msg_to_be_sent = cache_server.get(packet_number)
    lock.release()
    if (msg_to_be_sent!=-1):
        UDPClientSocket.sendto(msg_to_be_sent.encode(), serverAddressPort)
        try:
            ack = UDPClientSocket.recvfrom(bufferSize)
            if(ack[0].decode()=="Unreceived"):
                send_data_udp(threadNumber,packet_number)
        except:
            send_data_udp(threadNumber,packet_number)            
    else:
        lock.acquire()
        broadcast(packet_number,threadNumber)
        lock.release()
       

#broadcasting to all the clients
def broadcast(packet_number,targetThread):
    for client in range(0,noc):
        TCPClientSocket = TCP_broadcast_clients[client]             
        TCPClientSocket.send(str(packet_number).encode())
        message = TCPClientSocket.recv(bufferSize).decode()     
        if message != '-1':
            data_after_broadcast(client,targetThread)
            break

def data_after_broadcast(client,targetThread):
    UDPServerSocket = data_after_broadcast_sockets[client]
    try:
        bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    except:
        adr_ack = "127.0.0.1",35000+client
        UDPServerSocket.sendto("Unreceived".encode(),adr_ack)
        data_after_broadcast(client,targetThread)
    msg_received = (bytesAddressPair[0]).decode()
    adr = bytesAddressPair[1]
    UDPServerSocket.sendto("Received".encode(),adr)
    UDPClientSocket = UDP_sender_clients[targetThread]
    serverAddressPort = ("127.0.0.1", 23000+targetThread)
    UDPClientSocket.sendto(msg_received.encode(), serverAddressPort)
    try:
        ack = UDPClientSocket.recvfrom(bufferSize)
        if(ack[0].decode()=="Unreceived"):
            send_data_udp(threadNumber,packet_number)
    except:
        send_data_udp(threadNumber,packet_number)            
    cache_server.put(int(msg_received[:6]),msg_received)


#threading implementation
threads =[]
startTime_1 = time.time()
noOfThreads = noc
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=send_chunk, args=(i,))
    threads.append(x)
    x.start()    
for i in range(noOfThreads) :
    threads[i].join()
endTime_1 = time.time()
threads2 =[]
startTime_2 = time.time()
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=listen_tcp_request, args=(i,))
    threads2.append(x)
    x.start()    
for i in range(noOfThreads) :
    threads2[i].join()
endTime_2 = time.time()
for i in range(noc):
    TCP_broadcast_clients[i].send(str(-1).encode())
    TCP_broadcast_clients[i].close()
for i in range(noc):
    TCP_req_listner_sockets[i].close()

logstream.close()
