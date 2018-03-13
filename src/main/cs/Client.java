package main.cs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client extends Thread{
    private String ip;

    public synchronized void run(){
        Socket req = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try{
            req = new Socket(ip, 4200);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
        }
        catch (UnknownHostException u){
            u.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        finally {
            try{
                in.close();
                out.close();
                req.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new Client().start();
    }
}
