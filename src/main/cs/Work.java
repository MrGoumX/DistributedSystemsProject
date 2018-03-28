package main.cs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

public class Work extends Thread implements Serializable{
    ObjectInputStream in;
    ObjectOutputStream out;
    private String message;

    public Work(Socket connection){
        try{
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void run(){
        try {
            String bind = "Hello, I'm a worker";
            out.writeObject(bind);
            out.flush();
            message = (String) in.readObject();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        if(message.equalsIgnoreCase("Stats")){
            sendStats();
        }
        else if(message.equalsIgnoreCase("Close")){
            close();
        }
    }

    private void sendStats(){
        try{
            out.writeObject(HWInfo.getInfo());
            out.flush();
            out.writeInt(2*in.readInt());
            out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    private void close(){
        try {
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
