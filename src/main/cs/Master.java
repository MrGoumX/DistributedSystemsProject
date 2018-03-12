package main.cs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;

public class Master extends Thread{
    String ip;
    int cores;
    long ram;
    Master(String ip){
        this.ip = ip;
    }

    public synchronized void run(){
        Socket req = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            req = new Socket(ip, 8888);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
            cores = in.readInt();
            ram = in.readLong();
            out.writeInt(2);
            out.flush();
            System.out.println(in.readInt());
        }
        catch (UnknownHostException u){
            System.out.println("Unkown host");
        }
        catch(IOException e){
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

    public static void main(String[] args) throws InterruptedException {
        Thread[] conn = new Thread[args.length];
        for(int i = 0; i < args.length; i++){
            System.out.println(args[i]);
            conn[i] = new Master(args[i]);
        }
        for(Thread i : conn){
            i.start();
        }
        for(Thread i : conn){
            i.join();
        }
    }
}
