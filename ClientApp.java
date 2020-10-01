import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class ClientApp {
    private static ClientMiddleware myMiddleware = null;
    private static List<Integer> requestsID; //buffer for GetReply
    private static Random generator;
    private static int svcID = 40; //server's primality test

    //APPLICATION
    public static void main(String[] args){
        generator = new Random();
        requestsID = new ArrayList<>();

        int svcID = 40;
        int numOfRequestsToSend = 250; //default send session

        boolean appRun = true; //flag for application termination
        boolean withBlock = false; //block flag for GetReply

        Scanner userInput = new Scanner(System.in);
        String userChoice;

        int counter = 0; //general counter for SendRequest and GetReply

        //INITIALIZE MIDDLEWARE
        try {
            myMiddleware = new ClientMiddleware();
            myMiddleware.start();
        } catch (IOException e) {
            System.err.println(e.getMessage() + " application is closing");
            return;
        }


        while(appRun){
            System.out.println("Send Requests (s)\nGet Replies (g)\nExit (e)");
            System.out.println("Your choice: ");
            userChoice = userInput.nextLine();

            switch (userChoice) {
                //Send Requests
                case "s":
                    System.out.println("Thread pause");
                    myMiddleware.ThreadPause();

                    System.out.println("How many? ");
                    numOfRequestsToSend = userInput.nextInt();
                    counter = numOfRequestsToSend;

                    while (counter>0) {
                        SendRequest(svcID, GetRandom());
                        counter--;
                    }
                    System.out.println("Thread continue");
                    myMiddleware.ThreadContinue();
                    break;

                //Get Replies
                case "g":
                    System.out.println("With block?(y/n) ");
                    userChoice = userInput.nextLine();
                    withBlock = userChoice.equals("y");

                    counter = 0;
                    while (counter<requestsID.size() && requestsID.size()>0){
                        int reqID = requestsID.get(counter);
                        if (!GetReply(reqID, withBlock)) {
                            counter++;
                        }
                    }
                    break;

                //Exit application
                case "e":
                    appRun = false;
            }
        }

        //TERMINATE MIDDLEWARE
        myMiddleware.ThreadStop();
        try {
            myMiddleware.join(5000); //wait for thread to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        myMiddleware.CloseSocket();
    }


    //API METHODS
    public static void SendRequest(int svcID, String payload){
        int state;

        state = myMiddleware.SendRequest(svcID, payload);
        if(state == -1){
            System.err.println("Buffers error (maybe full)");
        }
        else{ //Middleware confirmed request
//            System.out.println("Middleware returned reqID " + state);
            requestsID.add(state);
        }

    }
    public static boolean GetReply(int reqID, boolean block){
        String reply = null;
        do {
            try {
                reply = myMiddleware.GetReply(reqID);
            } catch (Exception e) { //if request is discarded after certain tries
                System.out.println(reqID + e.getMessage());
                requestsID.remove(requestsID.indexOf(reqID)); //don't ask this request again
                return true;
            }
            if (reply != null) { //if request is answered
                System.out.println(reqID + " answer " + reply);
                requestsID.remove(requestsID.indexOf(reqID)); //don't ask this request again
                return true;
            }
        } while (reply == null && block);

//        System.out.println(reqID + " answer not available");
        return false;
    }

    //random integer generation in string form
    public static String GetRandom(){
        int randomNum = generator.nextInt(100000);
        return String.valueOf(randomNum);
    }
}