/*package main.inv;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;


public class SocketManager extends Thread {

    private ArrayList<Work> connections; // connections is a list with Works, each work used by specific worker.
    private Master master;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    /**
     * Variable Definition
     */

/*    public SocketManager(Socket socket, Master master, ArrayList<Work> conncentions) {
        this.socket = socket;
        this.master = master;
        this.connections = conncentions;
    }

    public synchronized void run(){
        try{
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeObject("Hello, I'm Master");
            out.flush();
            String diff = (String) in.readObject();
            if(diff.equalsIgnoreCase("Hello, I'm Worker")){
                worker();
                System.out.println("New worker connected!");
            }
            else if(diff.equalsIgnoreCase("Hello, I'm Client")){
                master.client();
                System.out.println("New client connected");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * The method that manages the communication and work between server and worker
     */
/*    private void worker(){
        try {
            // creates new work and bind it with its worker through socket.
            Work w = new Work(socket, out, in, "Stats");
            connections.add(w);
            w.start(); // read resources from worker
            w.join();

            if(connections.size() == 1) { // starts immediately  after first worker accept and distributes process to workers.
                new Thread("distribution_thread") {
                    public void run() {
                        master.dist();
                    }
                }.start();
            }
            else {
                master.morethantwo();
            }
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

}*/