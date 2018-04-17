
/*
* class Work used to parse matrices, factors and others parameters from master to worker in new thread.
* */

package main;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class Work extends Thread{
    /**
     * Variable Definition
     */
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    // message is a command(or an order) from master to worker.
    private String message = "";

    // POIS matrix contains csv elements of each point of interest.
    // U, I are User and Items(POIS) matrices.
    private RealMatrix POIS, U, I;

    private OpenMapRealMatrix Bin, C;

    // start, finish, startI, finishI = limits which specifies a limited area of U and I matrices to train this worker.
    private int cores, start, finish, k;
    private long ram;
    private double lamda; // lamda is L factor

    /**
     * Constructors
     */

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, OpenMapRealMatrix Bin, OpenMapRealMatrix C){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.Bin = Bin;
        this.C = C;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, RealMatrix U, RealMatrix I, int start, int finish, int k, double lamda){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.U = U;
        this.I = I;
        this.start = start;
        this.finish = finish;
        this.k = k;
        this.lamda = lamda;
    }

    /**
     * Starting Method
     */
    public synchronized void run(){
        if(message.equalsIgnoreCase("Stats")){
            getStats();
        }
        else if(message.equalsIgnoreCase("BinC")){
            sendBinC();
        }
        else if(message.equalsIgnoreCase("TrainU") || message.equalsIgnoreCase("TrainI")){
            sendMatrices();
        }
        else if(message.equalsIgnoreCase("Close")){
            close();
        }
    }



    /**
     * Receives from worker the PC specs of him
     */
    private void getStats(){
        try{
            out.writeObject(message);
            out.flush();
            ArrayList<String> specs = (ArrayList<String>) in.readObject();
            cores = Integer.parseInt(specs.get(0));
            ram = Long.parseLong(specs.get(1));
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * Sends matrices Bin and C to Workers
     */
    private void sendBinC() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(Bin);
            out.writeObject(C);
            out.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * Sends matrices to worker after first initialization and take as result matrices U and I after training.
     */
    private void sendMatrices() {
        try {
            out.writeObject(message);
            out.writeObject(U.getData());
            out.writeObject(I.getData());
            out.flush();
            out.writeInt(start);
            out.writeInt(finish);
            out.writeInt(k);
            out.writeDouble(lamda);
            out.flush();
            if (message.equalsIgnoreCase("TrainU")) {
                double[][] TU = (double[][]) in.readObject();
                U = MatrixUtils.createRealMatrix(TU); // update U.
            } else if (message.equalsIgnoreCase("TrainI")) {
                double[][] TI = (double[][]) in.readObject();
                I = MatrixUtils.createRealMatrix(TI); // update I.
            }
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * Closes socket
     */
    private void close() {
        try{
            out.writeObject(message);
            out.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * @return The worker connection
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * @return The worker output stream
     */
    public ObjectOutputStream getOut() {
        return out;
    }

    /**
     * @return The worker input stream
     */
    public ObjectInputStream getIn() {
        return in;
    }

    /**
     * @return The worker's cores
     */
    public int getCores() {
        return cores;
    }

    /**
     * @return The worker's ram in bytes
     */
    public long getRam() {
        return ram;
    }

    /**
     * @return The matrix of users
     */
    public double[][] getU() {
        return U.getData();
    }

    /**
     * @return The matrix of pois
     */
    public double[][] getI() {
        return I.getData();
    }

}
