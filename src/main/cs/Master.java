package main.cs;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class Master extends Thread{

    private String ip;
    private int cores, start, finish;
    private long ram;
    private final String check = "Hello, I'm a worker", message;
    private Socket req = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;
    private RealMatrix matrix;
    private double[][] U, I;
    private double lamda;

    Master(String ip, String message){
        this.ip = ip;
        this.message = message;
        try {
            req = new Socket(ip, 4200);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    Master(String ip, String message, RealMatrix matrix, int start, int finish, double lamda){
        this.ip = ip;
        this.message = message;
        this.matrix = matrix;
        this.start = start;
        this.finish = finish;
        this.lamda = lamda;
        try {
            req = new Socket(ip, 4200);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    Master(String ip, String message, int start,RealMatrix matrix, RealMatrix U, RealMatrix I, double lamda){
        this.ip = ip;
        this.message = message;
        this.start = start;
        this.matrix = matrix;
        this.U = U.getData();
        this.I = I.getData();
        this.lamda = lamda;
        try {
            req = new Socket(ip, 4200);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void run(){
        try {
            String bind = (String) in.readObject();
            if(bind.equalsIgnoreCase(check)){
                if(message.equalsIgnoreCase("Stats")) {
                    getStats();
                }
                else if(message.equalsIgnoreCase("InitDist")){
                    sendInitMatrices();
                }
                else if(message.equalsIgnoreCase("Dist")){
                    sendMatrices();
                }
                else if(message.equalsIgnoreCase("Close")){
                    close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void getStats(){
        try {
            out.writeObject(message);
            out.flush();
            ArrayList<String> HWInfo = (ArrayList<String>) in.readObject();
            cores = Integer.parseInt(HWInfo.get(0));
            ram = Long.parseLong(HWInfo.get(1));
        }
        catch (IOException io) {
            io.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void sendInitMatrices() {
        try{
            out.writeObject(message);
            out.flush();
            out.writeObject(matrix);
            out.writeDouble(lamda);
            out.flush();
            U = (double[][]) in.readObject();
            I = (double[][]) in.readObject();
            //U = (Array2DRowRealMatrix) in.readObject();
            //I = (Array2DRowRealMatrix) in.readObject();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }


    private void sendMatrices() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(matrix);
            out.writeObject(U);
            out.writeObject(I);
            out.writeDouble(lamda);
            out.flush();
            U = (double[][]) in.readObject();
            I = (double[][]) in.readObject();
            //U = (Array2DRowRealMatrix) in.readObject();
            //I = (Array2DRowRealMatrix) in.readObject();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void close() {
        try {
            in.close();
            out.close();
            req.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    public int getCores() {
        return cores;
    }

    public long getRam() {
        return ram;
    }

    public double[][] getU(){
        return U;
    }

    public double[][] getI(){
        return I;
    }

    public int getStart(){
        return start;
    }

    public int getFinish(){
        return finish;
    }

}
