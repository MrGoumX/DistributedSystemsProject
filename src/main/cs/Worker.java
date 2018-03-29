package main.cs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Worker extends Thread {
    ServerSocket server;
    Socket dis;
    private int port = 4200;
    public static void main(String args[]){
        new Worker().openServer();
    }

    private void openServer() {
        try{
            server = new ServerSocket(port, 3);
            while(true){
                dis = server.accept();
                //System.out.println("New info");
                Thread T = new Work(dis);
                T.start();
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
