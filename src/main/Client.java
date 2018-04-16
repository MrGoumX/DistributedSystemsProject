package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import static java.lang.System.exit;

public class Client extends Thread{
    /**
     * Variable Definition
     */
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int i, j;
    private String message;
    private boolean ctm;

    public Client(String master, int port, int i, int j){
        while(true) {
            try {
                socket = new Socket(master, port);
                if (socket.isConnected()) {
                    out = new ObjectOutputStream(socket.getOutputStream());
                    in = new ObjectInputStream(socket.getInputStream());
                    break;
                }
            } catch (IOException e) { // if connection failed, then wait for 2 seconds and try again.
                System.err.println("Server not live");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        this.i = i;
        this.j = j;
    }

    /**
     * Starting method
     */
    public synchronized void run(){
        while(true){
            try{
                message = (String) in.readObject();
            }
            catch (IOException | ClassNotFoundException e){
                e.printStackTrace();
                break;
            }
            if(message.equalsIgnoreCase("Hello, I'm Master")){
                ctm = true;
                recommendation(); // send message to master that you are worker.
            }
        }
    }

    private void recommendation(){
        try{
            out.writeObject("Hello, I'm Client");
            out.flush();
            out.writeInt(i);
            out.writeInt(j);
            out.flush();
            boolean trained = in.readBoolean();
            if(!trained){
                System.out.println((String) in.readObject());
            }
            else{
                System.out.println("Recommendation is: " + ((ArrayList<Integer>) in.readObject()).toString());
            }
            exit(0);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Client("172.16.1.71", 4200, 764, 5).start();
    }
}
