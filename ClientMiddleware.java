import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

public class ClientMiddleware extends Thread{
    //SOCKET
    private DatagramSocket socket; //client middleware socket
    private int socketTimeout = 1; //initial socketTimeout multiplier

    //CONNECTION
    String multicastMessage = "254"; //set multicast payload
    private InetAddress group = InetAddress.getByName("224.0.0.1"); //for all-hosts
    private SocketAddress serverSocket; //communication with server

    //RESTRICTIONS
    private int totalReqID = -1; //total requests from application
    private int rcvPacketSize = 10;
    private int bufferSize = 255; //size of all array-like buffers
    private int maxSendTries = 10; //send tries before quiting

    //BUFFERS
    private Integer[] totalReqIDBuffer; //for binding totalReqID to request and svcID
    private Map<Integer, String> storageBuffer; //for mapping totalReqID to request
    private Map<Integer, Integer> svcIDStorageBuffer; //for mapping totalReqID to wanted svcID
    private Map<Integer, String> requestReplyBuffer; //buffer for mapping totalReqID to request
    private Integer[] sendingBuffer; //buffer for binding totalReqID to serverReqID
    private Integer[] svcIDBuffer; //buffer for binding svcID to serverReqID
    private String[] reqStrBuffer; //buffer for binding request to serverReqID
    private int[] triesBuffer; //buffer to keep track of sending tries of each request from sendingBuffer

    //CONSTRUCTOR
    public ClientMiddleware() throws IOException{
        totalReqIDBuffer = new Integer[bufferSize];
        storageBuffer = new HashMap<>();
        requestReplyBuffer = new HashMap<>();
        svcIDStorageBuffer = new HashMap<>();
        sendingBuffer = new Integer[bufferSize];
        svcIDBuffer = new Integer[bufferSize];
        reqStrBuffer = new String[bufferSize];
        triesBuffer = new int[bufferSize];

//        group = InetAddress.getByName("224.3.29.71");
//        group = InetAddress.getByName("192.168.56.1");
        socket = new DatagramSocket();

        try {
            Multicast(multicastMessage);
        } catch (IOException e) {
            CloseSocket();
            e.printStackTrace();
            throw e;
        }
    }

    public void CloseSocket(){
        if(socket != null)
            socket.close();
    }

    //Find new server to begin communication
    public void Multicast(String multicastMessage) throws IOException {
        byte[] payloadBytes = multicastMessage.getBytes();

        int initialPort = 10000;
        DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, group, initialPort);
        byte[] rcvPacketBytes = new byte[rcvPacketSize];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);

        for(int i = 0; i<maxSendTries; i++){
            socket.setSoTimeout(socketTimeout*2000); //increase listening endurance
            try {
                socket.send(packet);
                socket.receive(rcv_packet);
                break;
            } catch (IOException e) { //multicast not received
                System.err.println("Multicast delay");
                socketTimeout++;
            }
            if(i==maxSendTries-1) //no server available, reset timeout
                socketTimeout = 1;
                socket.setSoTimeout(socketTimeout*200);
                throw new IOException("Multicast failed");
        }

        int state = Byte.toUnsignedInt(rcv_packet.getData()[0]);
        System.out.println("multicast received state " +state);

        //get communication identifier
        serverSocket = rcv_packet.getSocketAddress();
        System.out.println(rcv_packet.getSocketAddress());

        //reset delays and timeouts
        newSessionDelay = 1000;
        sendingDelay = 1000;
        newSessionTries = 0;
        newSession = false;
        socketTimeout = 1;
        socket.setSoTimeout(socketTimeout*200);
    }

    //API METHODS
    public int SendRequest(int svcID, String request){
        if(storageBuffer.size() == bufferSize) {
            return -1;
        }

        //request accepted
        totalReqID++;

        //bind totalReqID with given request and svcID
        storageBuffer.put(totalReqID, request);
        svcIDStorageBuffer.put(totalReqID, svcID);
        for(int i = 0; i<bufferSize; i++){
            if(totalReqIDBuffer[i] == null){
                totalReqIDBuffer[i] = totalReqID;
                break;
            }
        }

        return totalReqID;
    }
    public String GetReply(int appReqID) throws Exception {
        String replyToApp = SearchReplyBuffer(appReqID);

        if (replyToApp != null) {
            RemoveReplyFromBuffer(appReqID);
            return replyToApp;
        }else if(AppReqIDinSendingBuffer(appReqID)){ //if request is expected to be answered
            return null;
        }else if(SearchStorageBuffer(appReqID)){ //if request is expected to be answered
            return null;
        }else{ //if request is never to be answered
            throw new Exception(" request discarded");
        }
    }

    //REQUESTS METHODS
    public void StorageToSendingBuffer(){ //check storage for request to be sent
//        System.out.println("Get packets from storage");
        for(int i = 0; i<bufferSize; i++){ //get from storage as much as buffer's size
            if(totalReqIDBuffer[i] != null) { //if totalReqID is bound
                if (AddRequestEntry(totalReqIDBuffer[i], svcIDStorageBuffer.get(totalReqIDBuffer[i]), storageBuffer.get(totalReqIDBuffer[i])) != -1) {
                    //System.out.println("Added request entry " +totalReqIDBuffer[i] + " to sendingBuffer");
                    storageBuffer.remove(totalReqIDBuffer[i]);
                    svcIDStorageBuffer.remove(totalReqIDBuffer[i]);
                    totalReqIDBuffer[i] = null;
                } else {
                    System.err.println("Couldn't add request entry " +totalReqIDBuffer[i] + " to sendingBuffer");
                }
            }
        }
    }
    public int AddRequestEntry(Integer reqID, Integer svcID, String request){ //add request to sendingBuffer
        for(int serverReqID =0; serverReqID< bufferSize; serverReqID++){
            if(sendingBuffer[serverReqID] == null) {
                sendingBuffer[serverReqID] = reqID;
                svcIDBuffer[serverReqID] = svcID;
                reqStrBuffer[serverReqID] = request;
                //reset tries of serverReqID
                triesBuffer[serverReqID] = 0;
                return serverReqID;
            }
        }
        return -1;
    }
    public void RemoveAllRequestEntries(){
        for(int serverReqID=0; serverReqID<bufferSize; serverReqID++){
            RemoveRequestEntry(serverReqID);
        }
    }
    public void RemoveRequestEntry(int serverReqID){ //remove requests from sendingBuffer
        sendingBuffer[serverReqID] = null;
        reqStrBuffer[serverReqID] = null;
        svcIDBuffer[serverReqID] = null;
        triesBuffer[serverReqID] = 0;
    }
    public boolean ReqBufferIsEmpty(){
        for(int serverReqID = 0; serverReqID < bufferSize; serverReqID++){
            if(sendingBuffer[serverReqID] != null){
                return false;
            }
        }
        return true;
    }
    public int RequestsRemaining(){
        int requestRemaining = 0;
        for(int serverReqID = 0; serverReqID < bufferSize; serverReqID++){
            if(sendingBuffer[serverReqID] != null){
                requestRemaining++;
            }
        }
        return requestRemaining;
    }
    public boolean SearchStorageBuffer(int appReqID){
        return storageBuffer.containsKey(appReqID);
    }

    //MATCHING serverReqID-appReqID
    public Integer GetTotalReqIDFromBuffer(int serverReqID){ //get totalReqID for given sequence number
        if(serverReqID>=0 && serverReqID< sendingBuffer.length)
            return sendingBuffer[serverReqID]; //if null, means it was replied
        return null;
    }
    public boolean AppReqIDinSendingBuffer(int appReqID){ //get sequence number for given totalReqID
        for(int i = 0; i< bufferSize; i++){
            if(sendingBuffer[i] != null) {
                if (sendingBuffer[i] == appReqID) {
                    return true;
                }
            }
        }
        return false;
    }

    //REPLY BUFFER
    public boolean InsertToRequestReplyBuffer(int serverReqID, String replyStr){
        Integer appReqID = GetTotalReqIDFromBuffer(serverReqID);
        if(appReqID != null) {
            if (SearchReplyBuffer(appReqID) == null) {
                requestReplyBuffer.put(appReqID, reqStrBuffer[serverReqID]+" is prime: "+replyStr);
                return true;
            } else {
                System.err.println("Reply already exists for appReqID " + appReqID);
                return false;
            }
        }
        else {
            System.err.println("appReqID " + appReqID + " not found in InsertToRequestReplyBuffer");
            return false;
        }
    }
    public void RemoveReplyFromBuffer(int appReqID){
        requestReplyBuffer.remove(appReqID);
    }
    public String SearchReplyBuffer(int appReqID){
        String replyToApp;
        replyToApp = requestReplyBuffer.get(appReqID);
        return replyToApp;
    }

    //THREAD SUBCLASS
    private int sendingDelay = 1000;
    private int newSessionDelay = 1000;
    private int newSessionTries = 0;
    private boolean threadExit = false;
    private boolean appSending = false;
    private boolean serverBusy = false;
    private boolean newSession = false;
    private boolean needMulticast = false;

    //Sending acks, specifically newSession ACK (stateCode == 1)
    public void SendStateCode(int stateCode){
        byte[] stateCodeBytes = ByteBuffer.allocate(4).putInt(stateCode).array();

        byte[] packetBytes = new byte[1];

        //Construct packet payload
        System.arraycopy(stateCodeBytes, 3, packetBytes, 0, 1);
        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, serverSocket);

        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //Sending acks, remain server communication (stateCode == 2)
    public void SendBeacon(int stateCode){
        byte[] stateCodeBytes = ByteBuffer.allocate(4).putInt(stateCode).array();
        byte[] packetBytes = new byte[stateCode];

        //Construct packet payload
        System.arraycopy(stateCodeBytes, (4-stateCode), packetBytes, 0, stateCode);
        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, serverSocket);

        byte[] rcvPacketBytes = new byte[rcvPacketSize];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);

        for(int i = 0; i < maxSendTries; i++){
            try {
                socket.setSoTimeout(socketTimeout*1000); //increase listening endurance
                socket.send(packet);
                socket.receive(rcv_packet);

                if(rcv_packet.getLength() == 1){
                    int state = Byte.toUnsignedInt(rcv_packet.getData()[0]);
//                    System.out.println("Received state "+state);
                    if(state == stateCode){
                        needMulticast = false;
                        socket.setSoTimeout(socketTimeout*50);
//                        System.out.println("Server is still listening");
                        break;
                    }
                }
            } catch (IOException e) {
                System.err.println("Beacon didn't received");
                socketTimeout++;
            }
            if(i==maxSendTries-1) {
                socketTimeout = 1;
                needMulticast = true;
            }
        }
    }

    public void SendPacketsFromReqBuffer(){
        System.out.println("Sending packets");
        for(int serverReqID = 0; serverReqID < bufferSize; serverReqID++){
            if(sendingBuffer[serverReqID] != null){ //if sequence number is not answered or discarded
                if(triesBuffer[serverReqID] <maxSendTries) {
                    SendPacket(svcIDBuffer[serverReqID], serverReqID, triesBuffer[serverReqID],reqStrBuffer[serverReqID]);
                    triesBuffer[serverReqID]++;
                }
                else{
                    System.out.println("Discarding request with serverReqID " + serverReqID);
                    RemoveRequestEntry(serverReqID);
                }
            }
        }
    }

    public void SendPacket(int svcID, int serverReqID, int reqTry, String request){
        if(reqTry == maxSendTries-1)
            System.out.println("Ask for sequence " + serverReqID + " last try " + reqTry);
//        System.out.println("Socket " + serverSocket);
        byte[] svcIDBytes = ByteBuffer.allocate(4).putInt(svcID).array();

        byte[] serverReqIDBytes = ByteBuffer.allocate(4).putInt(serverReqID).array();

        byte[] reqTryBytes = ByteBuffer.allocate(4).putInt(reqTry).array();

        byte[] payloadBytes = request.getBytes();

        byte[] packetBytes = new byte[payloadBytes.length + 3];

        //Construct packet
        System.arraycopy(svcIDBytes, 3, packetBytes, 0, 1);
        System.arraycopy(serverReqIDBytes, 3, packetBytes, 1, 1);
        System.arraycopy(reqTryBytes, 3, packetBytes, 2, 1);
        System.arraycopy(payloadBytes, 0, packetBytes, 3, payloadBytes.length);

        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, serverSocket);

        try {
            socket.send(packet);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
    public void ReceivePacket(){
        byte[] rcvPacketBytes = new byte[rcvPacketSize];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);
        try {
            socket.receive(rcv_packet);
//            System.out.println("from " + rcv_packet.getSocketAddress());
            //PACKET HAS PAYLOAD
            if(rcv_packet.getLength() >1){
                /*if(serverSocket != rcv_packet.getSocketAddress())
                    serverSocket = rcv_packet.getSocketAddress();*/

                int serverReqID = Byte.toUnsignedInt(rcv_packet.getData()[0]);
                String replyStr = new String(rcv_packet.getData(), 1, rcv_packet.getLength()-1);

                Integer appReqID = GetTotalReqIDFromBuffer(serverReqID); //if return notNull, means it is unanswered
//                System.out.println("Received serverReqID"+serverReqID+" appReqID "+ appReqID +" Received str "+ replyStr);
                if(appReqID != null ) {
                    if(SearchReplyBuffer(appReqID) == null) {
//                        System.out.println("reply received " + serverReqID + " replyStr: " + replyStr);
                        if(InsertToRequestReplyBuffer(serverReqID, replyStr)){
                            System.out.println("Inserted reply " + serverReqID + " appReqID: " + GetTotalReqIDFromBuffer(serverReqID));
                            RemoveRequestEntry(serverReqID);
                            if (ReqBufferIsEmpty()) { //if buffer had requests but emptied
                                newSession = true;
                            }
                        }
                    }
                }
            }

            //MIDDLEWARE COMMUNICATION
            else if(rcv_packet.getLength() == 1){
                int state = Byte.toUnsignedInt(rcv_packet.getData()[0]);
//                System.out.println("Received state "+state);
                if(state == 0){ //
                    System.err.println("Server busy");
                    serverBusy = true;
                }
                else if(state == 1){ //ACK for new session //remove old requests //refill sendingBuffer
                    RemoveAllRequestEntries();
                    System.out.println("Received newSessionACK");
                    newSessionDelay = 1000;
                    sendingDelay = 1000;
                    newSession = false;
                    newSessionTries = 0;
                }
                else if(state == 2){
                    needMulticast = false;
                    System.out.println("Server is still listening");
                }
                else if(state == 254){ //multicast from other server
                    System.out.println("Server multicast ignored");
                }
                else {
                    System.out.println("Service " + state + " doesn't exist");
                    //functionality for removing requests with that svcID
                    /*for(int i = 0; i < bufferSize; i++){
                        if(svcIDBuffer[i] == state)
                            RemoveRequestEntry(i);
                    }*/
                }
            }
            else
                System.err.println("Empty packet");
        } catch (IOException ignored) {
        }
    }

    public void run(){
        int minimumReceives = 5;

        while(!threadExit){
            //adjust loop size for listening
            int receiveCounter = RequestsRemaining() + minimumReceives;
            for (int i = 0; i < receiveCounter; i++) {
                if (threadExit)
                    break;
                ReceivePacket();
            }

            if (threadExit)
                break;

            //mutex-like flag with application
            //if app is requesting, don't refill from storage
            if(!appSending) {
                if (ReqBufferIsEmpty()) {
//                    System.out.println("in ReqBufferIsEmpty condition");
                    if (threadExit)
                        break;
                    if (newSession) { //send newSession for refilling from storage and flushing server's buffers
//                        System.out.println("in newSession condition");
                        if(newSessionTries >= maxSendTries)
                        {
                            System.out.println("Server doesn't respond");
                            try {
                                Multicast(multicastMessage);
                                System.out.println("Connected to new server");
                                continue;
                            } catch (IOException e) {
                                System.err.println(e.getMessage());
//                                threadExit = true;
                                System.err.println("404 server not found");
                                continue;
                            }
                        }

                        System.out.println("Sending newSession");
                        try {
                            Thread.sleep(newSessionDelay);
                            SendStateCode(1);
                            newSessionTries++;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        newSessionDelay += 500;
                    } else { //if about to refill from storage
//                        System.out.println("in !newSession condition");
                        SendBeacon(2); //confirm that server is alive
                        if(needMulticast){
                            System.out.println("Server doesn't respond");
                            try {
                                Multicast(multicastMessage);
                                System.out.println("Connected to new server");
                            } catch (IOException e) {
                                System.err.println(e.getMessage());
//                                threadExit = true;
                                System.err.println("404 server not found");
                                continue;
                            }
                        }
                        StorageToSendingBuffer();
                    }
                }
            }

            if (threadExit)
                break;

            if (!ReqBufferIsEmpty()) { //if ready to send more requests
//                System.out.println("in !ReqBufferIsEmpty condition");
                if (threadExit)
                    break;
                if (!serverBusy) { //if server is available, reset delay and send
//                    System.out.println("in !serverBusy condition");
                    sendingDelay = 1000;
                    try {
                        SendPacketsFromReqBuffer();
                        Thread.sleep(sendingDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (ReqBufferIsEmpty()) { //if buffer had requests but emptied
                        sendingDelay = 1000;
                        newSession = true;
                    } else { //else keep current session until buffer is empty
//                        sendingDelay += 1000;
                        sendingDelay += 500;
                    }
                } else { //if server is full, increase delay and suppose he is not full
                    System.out.println("in serverBusy condition");
                    sendingDelay += 500;
                    serverBusy = false; //wait for a delay until resend
                    try {
                        Thread.sleep(sendingDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        System.out.println("thread exited loop");
    }
    public void ThreadPause(){
        appSending = true;
    }
    public void ThreadContinue(){
        appSending = false;
    }
    public void ThreadStop(){ //send cancellation so that server flushes your replies
        System.out.println("Send cancellation");
        SendStateCode(1);
        threadExit = true;
    }
}