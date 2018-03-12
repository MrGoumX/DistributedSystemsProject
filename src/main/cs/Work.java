package main.cs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Work extends Thread{
    ObjectInputStream in;
    ObjectOutputStream out;

    public Work(Socket connection){
        try{
            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void run(){
        try{
            int cores = Runtime.getRuntime().availableProcessors();
            long ram = Runtime.getRuntime().freeMemory();
            out.writeInt(cores);
            out.writeLong(ram);
            out.flush();
            out.writeInt(2*in.readInt());
            out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        finally {
            try{
                in.close();
                out.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
