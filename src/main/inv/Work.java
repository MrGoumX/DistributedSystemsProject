
/*
* class Work used to parse matrices, factors and others parameters from master to worker in new thread.
* */

package main.inv;

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

    private boolean isInitializedUI = false;

    // message is a command(or an order) from master to worker.
    private String message = "";

    // POIS matrix contains csv elements of each point of interest.
    // U, I are User and Items(POIS) matrices.
    private RealMatrix POIS, U, I;

    // start, finish, startI, finishI = limits which specifies a limited area of U and I matrices to train this worker.
    private int cores, start, finish, startI, finishI;
    private long ram;
    private double lamda; // lamda is L factor

    /**
     * Constructors
     */
    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in){
        this.socket = socket;
        this.out = out;
        this.in = in;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, RealMatrix POIS, int start, int finish, int startI, int finistI, double lamda){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.POIS = POIS;
        this.start = start;
        this.finish = finish;
        this.startI = startI;
        this.finishI = finistI;
        this.lamda = lamda;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, RealMatrix POIS, int start, int finish, RealMatrix U, RealMatrix I, double lamda){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.POIS = POIS;
        this.start = start;
        this.finish = finish;
        this.U = U;
        this.I = I;
        this.lamda = lamda;
    }

    /**
     * Starting Method
     */
    public synchronized void run(){
        if(message.equalsIgnoreCase("Stats")){
            getStats();
        }
        else if(message.equalsIgnoreCase("InitDist")){ // "InitDist" message represents only the first time training.
            sendInitMatrices();
            isInitializedUI = true;
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
     * Sends matrices to worker only for first initialization and training, and take as result matrices U and I after training.
     */
    private void sendInitMatrices() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(POIS);
            out.writeDouble(lamda);
            out.writeInt(start);
            out.writeInt(finish);
            out.writeInt(startI);
            out.writeInt(finishI);
            out.flush();
            U = (RealMatrix) in.readObject();
            I = (RealMatrix) in.readObject();
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * Sends matrices to worker after first initialization and take as result matrices U and I after training.
     */
    private void sendMatrices() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(POIS);
            out.writeObject(U); // U and I contains value of first training within sendInitMatrices().
            out.writeObject(I);
            out.writeDouble(lamda);
            out.writeInt(start);
            out.writeInt(finish);
            out.flush();
            if(message.equalsIgnoreCase("TrainU")){
                U = (RealMatrix) in.readObject(); // update U.
            }
            else if(message.equalsIgnoreCase("TrainI")){
                I = (RealMatrix) in.readObject();// update I.
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
            out.close();
            in.close();
            socket.close();
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

    /**
     * @return if U I and k have initialized.
     */
    public boolean isInitializedUI() {
        return isInitializedUI;
    }
}
