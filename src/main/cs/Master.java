package main.cs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;

public class Master extends Thread{
    private String ip;
    private int cores;
    private long ram;
    Master(String ip){
        this.ip = ip;
    }

    public synchronized void run(){
        Socket req = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            req = new Socket(ip, 4200);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
            cores = in.readInt();
            //System.out.println(cores);
            ram = in.readLong();
            //System.out.println(ram);
            out.writeInt(2);
            out.flush();
            System.out.println(in.readInt());
        } catch (UnknownHostException u) {
            System.out.println("Unkown host");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                req.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public int getCores(){
        return cores;
    }

    public long getRam(){
        return ram;
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
