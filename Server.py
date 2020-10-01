import socket
import struct
import threading
import signal
from time import sleep
import sys,os

# Server buffers size
SIZE = 255
# MAX_CLIENTS = 5
# service that i currently serve
requested_service = 40
# clients socket list
cl_sockets = []
# until  the service is registered
service_on = False
Requests = 0
# the ticket  for the service
req_id = -1
buf = ""
buf_len = -1

# reply list for the application
app_replies = []

app_buffer = []
app_address_buffer = []
# list of requests currently under process
proccesing_buffer = [None] * SIZE
proccesing_address_buffer = [None] * SIZE
# reply list to delivery
reply_buffer = []
reply_address_buffer = []

number_of_requests = []
#addresses tha i currently serve
addresses = []

total_reqs = 0
# socket list that the server listens to
bigsock = []
# socket apo to opoio apadw se olus tus pelates kai apo dw vgainei kai to 2o meros tu unique id
#one socket to reply to the clients
reply_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
reply_socket.sendto(bytes([1]),("8.8.8.8",12000))
my_socket_number = reply_socket.getsockname()[1]
# each client's delay
delay = []


# server's analyzer thread check what kind of message you received and handle it accordingly
def thread_function(sem):
    global req_id, buf, buf_len, total_reqs,listen
    print("Threadstarting")

    flag = 0

    while True:

        if len(cl_sockets) is not 0:

            for size in range(len(cl_sockets)):
                number_of_requests.append(0)
                addresses.append(cl_sockets.pop(0))

                bigsock.append(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                bigsock[len(bigsock) - 1].sendto(bytes([254]), addresses[len(addresses) - 1])

                print(addresses[len(addresses) - 1])
                delay.append(0)
                bigsock[len(bigsock) - 1].settimeout(0.3)

        for counter in range(len(bigsock)):

            # give requests to the app
            if Requests != 0 and len(app_buffer) > 0:
                sem.acquire(blocking=False)
                req_id, buf, buf_len = app_requests()
                sem.release()

            # transfer the replies to reply buffer
            for k in range(len(app_replies)):
                for j in range(len(proccesing_buffer)):
                    # se periptwsi cancellation
                    if proccesing_buffer[j] is not None and app_replies[k][0] == proccesing_buffer[j][0]:
                        rep_size = app_replies[k][1]
                        reply = app_replies[k][2][0:rep_size]
                        answer = proccesing_buffer[j][1][0], reply
                        reply_buffer.append(answer)
                        proccesing_buffer[j] = None
                        reply_address_buffer.append(proccesing_address_buffer[j])
                        proccesing_address_buffer[j] = None
            # reply to clients
            if len(reply_buffer) > flag:
                for k in range(flag,len(reply_buffer)):
                    #print("reply to send ", reply_buffer[k][0], reply_buffer[k][1], "address", reply_address_buffer[k])
                    reply_socket.sendto(bytes([reply_buffer[k][0]]) + bytes(reply_buffer[k][1], "utf-8"),
                                    reply_address_buffer[k])

            flag = len(reply_buffer)
            # listen to clients
            my_data, to_send_address = recmessage(bigsock[counter])

        # check if a client is delaying
            if to_send_address is None:
                delay[counter] = delay[counter] + 1

                if delay[counter] == 75:
                    print("CLIENT IS DELAYING-------------------------------------------------------")
                    bigsock[counter].settimeout(0.2)
                    continue
                # an argisei polu kovetai
                if delay[counter] == 110:
                    print("client is drinking too much coffee closing,messages received from  ", addresses[counter],
                          "=", number_of_requests[counter])
                    # delete him
                    clear_buffers(app_address_buffer, app_buffer, addresses[counter], True)
                    clear_buffers(proccesing_address_buffer, proccesing_buffer, addresses[counter], False)
                    clear_buffers(reply_address_buffer, reply_buffer, addresses[counter], True)

                    number_of_requests.pop(counter)
                    addresses.pop(counter)
                    bigsock.pop(counter)
                    delay.pop(counter)
                    break
                continue
            # heartbits from clients who do not interact
            if len(my_data) == 2:
                bigsock[counter].sendto(bytes([2]), to_send_address)
                delay[counter] = 0
                continue

        # renew session with a client,all gone well until now lets begin again
            if len(my_data) == 1:
                clear_buffers(app_address_buffer, app_buffer, to_send_address, True)
                print("new session-------------------------------------------------------------------")
                clear_buffers(proccesing_address_buffer, proccesing_buffer, to_send_address, False)
                clear_buffers(reply_address_buffer, reply_buffer, to_send_address, True)
                bigsock[counter].sendto(bytes([1]), to_send_address)
                delay[counter] = 0
                continue

            # if a client resent e message check if you have the answer and give it to him else you never got it the first time
            if int(my_data[2]) != 0:
                delay[counter] = 0
                if resend_packet(bigsock[counter], to_send_address, int(my_data[1])):
                    continue

                if packetfinder(int(my_data[1]), to_send_address, app_address_buffer, app_buffer, False):
                   # print("to vrika sto app buffer")
                    continue

                if packetfinder(int(my_data[1]), to_send_address, proccesing_address_buffer, proccesing_buffer, True):
                    #print("to vrika sto process")
                    continue

                if packetfinder(int(my_data[1]), to_send_address, reply_address_buffer, reply_buffer, False):
                    # Proliptika den bainei pote edw giati an to vrike sto reply buffer to steile kiolas
                    continue
            # SENDING that i am busy ,server buffers reaching maximum
            if len(app_buffer) >= SIZE:
                # print("Sending that i am busy",len(app_buffer))
                bigsock[counter].sendto(bytes([0]), to_send_address)
                continue

            #new message check if it is for a registered service
            # an den einai gia to service pu eksipiretw i to service mu einai kleisto
            if int(my_data[0]) != 40 or service_on is False :
                # print("sending service not avalailable message")
                bigsock[counter].sendto(bytes([my_data[0]]), to_send_address)
                continue

            # store message and client data
            number_of_requests[counter] += 1
            to_store = int(my_data[1]), my_data[3:].decode()
            app_buffer.append(to_store)
            app_address_buffer.append(to_send_address)
            total_reqs = total_reqs + 1
            delay[counter] = 0


def resend_packet(sender, receiver, seqN):
    for j in range(len(reply_buffer)):
        if reply_address_buffer[j] == receiver:
            if int(reply_buffer[j][0]) == seqN:
                sender.sendto(bytes([reply_buffer[j][0]]) + bytes(reply_buffer[j][1], "utf-8"),
                              receiver)
                # print("to steila pali",seqN)
                return True
    # print("de to vrika",seqN)
    return False

# deliver the message to your service
def app_requests():
    global Requests
    my_id = -1
    if Requests != 0 and service_on:
        if len(app_buffer) is not 0:
            temp = app_buffer[0]

            for j in range(0, SIZE):
                if proccesing_buffer[j] is None:
                    proccesing_buffer[j] = total_reqs, app_buffer.pop(0)
                    proccesing_address_buffer[j] = app_address_buffer.pop(0)
                    my_id = total_reqs
                    break
            if my_id != -1:
                Requests -= 1
                return my_id, int(temp[1]), len(temp) - 1
    return -1, -1, -1


def packetfinder(seqN, sender_addr, ad_buffer, data_buffer, proc_buf):
    if proc_buf:
        for j in range(len(ad_buffer)):
            if ad_buffer[j] == sender_addr:
                if int(data_buffer[j][1][0]) == seqN:
                    return True
        return False

    for j in range(len(ad_buffer)):
        if ad_buffer[j] == sender_addr:
            if int(data_buffer[j][0]) == seqN:
                return True
    return False


def clear_buffers(ad_buffer, data_buffer, sender_address, pop):
    if pop:
        to_delete = 0
        for j in range(len(ad_buffer)):
            if ad_buffer[j] == sender_address:
                to_delete += 1
        for j in range(to_delete):
            for l in range(len(ad_buffer)):
                if ad_buffer[l] == sender_address:
                    ad_buffer.pop(l)
                    data_buffer.pop(l)
                    break
        return

    for j in range(len(ad_buffer)):
        if ad_buffer[j] == sender_address:
            ad_buffer[j] = None
            data_buffer[j] = None

    return


def recmessage(rec_addr):
    try:
        rec_data, to_send_address = rec_addr.recvfrom(1024)
        return rec_data, to_send_address

    except Exception as ex:
        if ex.__class__ is socket.timeout:
            return None, None


def register(svcid):
    global service_on

    if svcid == 40:
        service_on = True


def unregister(svcid):
    global service_on
    if svcid == 40:
        service_on = False

# sinartisi api zita kleise to semaforo kai perimene
def getRequest(svc, sem):  # buf, length):

    global Requests
    global requested_service
    register(svc)
    Requests += 1
    # print("application going to block ")
    try:
        sem.acquire()
    except Exception as ex:
        print("exception never occured before", ex.__class__)
        return

    return req_id, buf, buf_len


def sendreply(reqid, reply, size):
    temp = reqid, size, reply
    app_replies.append(temp)

#simple function for my service it can be whatever you want
def is_prime(n):

    # make sure n is a positive integer
    n = abs(int(n))

    # 0 and 1 are not primes
    if n < 2:
        return False

    # 2 is the only even prime number
    if n == 2:
        return True

    # all other even numbers are not primes
    if n % 2 == 0:
        return False

    # range starts with 3 and only needs to go up
    # the square root of n for all odd numbers
    for x in range(3, int(n**0.5) + 1, 2):
        if n % x == 0:
            return False

    return True


def application():
    register(40)
    req_list = []
    buf_list = []
    sem = threading.Semaphore(0)
    server = threading.Thread(target=server_init, args=(sem,), daemon=True)
    print(" application starting server begin")
    server.start()

    while True:
    #for j in range(50):
        # print("requesting from server")
        temp, request, lent = getRequest(40, sem)

      #  print("PIRA AFTO TO REQUEST",temp,int(request))
        if temp != -1:
            req_list.append(temp)
            buf_list.append(buf)
            if is_prime(int(request)):
                sendreply(temp, "YES", len("YES"))
            else:
                sendreply(temp, "NO", len("NO"))
                sleep(3)

    unregister(40)
    #server.join()


def handler(signum, frame):
    print('Signal handler called with signal', signum)

    for j in range(len(bigsock)):
        bigsock[j].close()

    # ENIMERWSE TON EPOMENO OTI PREPEI NA GINEI PRWTOS (DILADI NA MIN BEI SE IDLE MODE MOLIS TELEIWSEI)
    if child_server is not None:
        server_comm_sock.sendto(bytes("YOU_FIRST", "utf-8"), child_server)
    if server_to_open is not None:
        server_comm_sock.sendto(bytes("YOU_FIRST","utf-8"), server_to_open)

    server_comm_sock.close()
    reply_socket.close()
    sock.close()
    print("server received ", total_reqs, "messages")

    # se periptwsi thanatu apothikefse ton reply buffer se arxeio (at most once)
    f = open("server.txt", "w")
    for j in range(len(reply_buffer)):
        f.write(str(reply_buffer[j][0]))
        f.write(",")
        f.write(str(reply_buffer[j][1]))
        f.write(",")
        f.write(str(reply_address_buffer[j][0]))
        f.write(",")
        f.write(str(reply_address_buffer[j][1]))
        f.write(",")
    f.close()
    print("server closing")
    exit()


# multicast socket  server-client to find the servers
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# handler ,if the server must die clean up some mess
signal.signal(signal.SIGINT, handler)
# socket for the intercom between servers
server_comm_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# metavliti tu prwtu
first_flag = False
# wanna be paidi su socket-address
server_to_open = None
# socket address aftu pu se ksipnise
father_server = None
# unique server id.it occurs from your ip and your reply-socket
my_unique_id = None
# socket adress tu paidiu su
child_server = None

# if you the first server begin else wait for server to signal,different kinds of failures can occur
def server_start():
    global first_flag, father_server

    private_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    private_sock.settimeout(3)
    # steile simadia zwis sto multicast diktio twn server kai perimene apadisi an pareis apadisi WAIT bes se idle mode
    # an pareis apadisi YOU i YOU_FIRST ksekina i ksekina prwtos
    # an den pareis apadisi eisai o prwtos sixaritiria
    for temp in range(3):
        private_sock.sendto(bytes(str(my_unique_id), "utf-8"), ("224.3.29.71", 10001))
        co_data, co_addres = recmessage(private_sock)

        if co_data is not None:
            result = co_data.decode().split("'")
        else:
            continue

        if result[0] == "WAIT":
            print(co_data.decode())
            break
    # an eimai o prwtos server sinexizw alliws blokarw
    if co_addres is None:
        print("i'm first")
        first_flag = True
        server_comm_sock.settimeout(1)
    else:
        # block until someone wakes me up
        print("I AM WAITING")
        temp = 3
        while co_data is None or (co_data.decode() != "YOU" and co_data.decode() != "YOU_FIRST"):
            temp += 2
            private_sock.sendto(bytes(str(my_unique_id), 'utf-8'), co_addres)
            private_sock.settimeout(temp)
            co_data, co_address = recmessage(private_sock)
            if co_data is not None:
                print(co_data.decode())

            if temp >= 50:
                exit()
        if co_data.decode() == "YOU_FIRST":
            print("IM FIRST")
            first_flag = True
            server_comm_sock.setblocking(False)
            server_comm_sock.settimeout(1)
        else:
            #print("AFTOS ME KSIPNISE ",co_addres)
            co_data,co_addres=recmessage(private_sock)
            if co_data is not None:
                print("I WOKE UP BY HIM",co_data.decode(),co_addres)
                server_comm_sock.sendto(bytes("ACK", 'utf-8'), co_addres)
                father_server = co_addres
            server_comm_sock.setblocking(False)
            server_comm_sock.settimeout(2)
    return

# vres mia sigkekrimeni address se mia lista
def address_finder(address_buf,address_to_find):

    for j in range(len(address_buf)):
        if address_buf[j] == address_to_find:
            return True

    return False


# des an o client steilei diplotipo multicast
def multicast_duplicate(to_add_address):
    for j in range(len(addresses)):
        if addresses[j] == to_add_address:
            return True

    return False


# Sinartisi gia na ksekinisei o server,ginetai xamos edw
def server_init(sem):
    global father_server, my_unique_id, server_to_open, first_flag, child_server
    wake_another = True
    my_life = 0
    server_list = []
    times_to_wake_up = 0
    child_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    father_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    child_socket.sendto(bytes([1]), ("8.8.8.8", 12000))
    # print("my socket for the child",child_socket.getsockname())
    child_socket.settimeout(2)
    father_socket.settimeout(2)
    child_delay = 0
    father_delay = 0
# ELENGKSE AN TERMATISES KALA,AN OXI VALE OTI APANTISEIS EXEIS PISW STUS BUFFER
    try:
        # f=open("server.txt","r")
        with open('server.txt','r') as myFile:
            text = myFile.read()
        result = text.split(",")
        statinfo = os.stat('server.txt')
        print("BOOT AND FOUND THESE BYTES",statinfo.st_size)
        j=0
        while j < len(result)-1:
            reply_buffer.append((int(result[j]), result[j+1]))
            j = j+2
            reply_address_buffer.append((result[j], int(result[j+1])))

            if not address_finder(addresses, (result[j], int(result[j+1]))):
                print("vazw to pelati mesa",(result[j], int(result[j+1])))
                addresses.append((result[j], int(result[j+1])))
                bigsock.append(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                bigsock[len(bigsock) - 1].settimeout(0.3)
                number_of_requests.append(0)
                delay.append(0)
            j = j+2

    except Exception as x:
        print("problem with server's file ",x)

    # server multicast
    # get my ip
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
    IP = s.getsockname()[0]
    s.close()

    my_unique_id = (str(IP), str(my_socket_number))
    print("my_unique_id",my_unique_id)
    multicast_group = "224.3.29.71"
    server_address = ('', 10001)
    server_comm_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_comm_sock.bind(server_address)

    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    server_comm_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    server_start()
    # server- client multicast socket
    multicast_group = "224.0.0.1"  # "224.3.29.71"
    server_address = ('', 10000)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(server_address)
    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.settimeout(1)

    thread_id = threading.Thread(target=thread_function, args=(sem,), daemon=True)
    thread_id.start()

    while True:
        address = None
        # stelne heart bits se afton pu se sikwse gia na min xreiastei na sikwsei allo server
        # an den su steilei pisw den egine kati logika pethane

        if first_flag is False and father_server is not None:
            father_socket.sendto(bytes([1]),father_server)
            fd_data, fd_address = recmessage(father_socket)
            if fd_data is not None and fd_data.decode() == "YOU_FIRST":
                print("im first again")
                first_flag = True
        # an exeis ftasei sto 70% ksipna ton epomeno
        if len(app_buffer) >= (3*SIZE/4) and server_to_open is not None and wake_another is True:

            if times_to_wake_up < 5:
                print("waking up next server")
                child_socket.sendto(bytes("YOU", "utf-8"), server_to_open)
                #print(child_socket.getsockname())
                child_data,child_address = recmessage(child_socket)
                times_to_wake_up += 1
                #print(child_data.decode())
                if child_data is not None and child_data.decode() == "ACK":
                    print("HE WOKE UP")
                    child_server = child_address
                    wake_another = False

                elif times_to_wake_up == 5:
                    server_to_open = None
                    times_to_wake_up = 0
        # an eisai katw apo 80% se xwritikotita peripu deksu kainurgius client
        if len(app_buffer) < 5*SIZE/6:
            data, address = recmessage(sock)

        if address is not None:
            if not multicast_duplicate(address):
                print("append client",data.decode(), address)
                cl_sockets.append(address)

        # akuw gia allus servers kai tus vazw mesa
        co_data, co_addres = recmessage(server_comm_sock)
        if co_addres is not None:
            if len(co_data.decode()) > 5:
                result = co_data.decode().split("'")
                if result[1] != str(my_unique_id[0]) or result[3] != str(my_unique_id[1]):

                    if not address_finder(server_list, co_addres):
                        print("having another available server")
                        server_list.append(co_addres)
                        print(co_addres)
                        server_comm_sock.sendto(bytes("WAIT","utf-8"),co_addres)
                    # kathe kainurgio server pu bainei kanton wanna be paidi su
                    if wake_another:
                        #print("he is my child now")
                        server_to_open = co_addres
        # an exeis ksipnisei paidi aku ta heartbits tu i aku se periptwi pu kleisei an den stelnei logika pethane
        # kopston
        if wake_another is False:
            child_data, child_address = recmessage(child_socket)
            if child_data is not None:
                if child_data.decode() == "CL":
                    print(" my child closing")
                    child_socket.sendto(bytes("ACK", "utf-8"), child_address)
                    # for j in range(len(server_list)):
                    #     if server_list[j] == server_to_open:
                    #         server_list.pop(j)
                    #         break
                    child_server = None
                    server_to_open = None
                    server_list = []
                    wake_another = True
                    child_delay = 0

                child_server = child_address
                child_socket.sendto(bytes([1]) , child_address)
            else:
                child_delay += 1
                if child_delay == 20:
                    server_to_open = None
                    wake_another = True
                    print("FORCE CLOSING CHILD")
                    child_delay = 0
                    server_list = []
                    server_to_open = None
                    times_to_wake_up = 0
                    wake_another = True
                    child_server= None
        # an den eisai o prwtos kai den iparxei duleia pigaine gia kafe
        if len(addresses) == 0 and first_flag is False:
            my_life += 1

        if my_life >= 10 and len(addresses) == 0 and first_flag is False:
            print("server going back to sleep")
            #steile se afton pu se anoikse oti kleineis
            father_socket.settimeout(5)
            father_socket.sendto(bytes("CL", "utf-8"), father_server)
            co_data, co_addres = recmessage(father_socket)
            if co_data is not None and co_data.decode() == "ACK":
                server_list = []
                father_socket.settimeout(1)
                father_delay = 0
                print("sleeping")
            else:
                print("parent process probably died sleeping anywho")
                server_list = []
                father_socket.settimeout(1)
                father_delay = 0
                my_life = 0
            server_start()


application()

