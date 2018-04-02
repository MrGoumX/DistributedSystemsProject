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
    private String message = "";
    private RealMatrix matrix, U, I;
    private int cores, start, finish;
    private long ram;
    private double lamda;

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

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, RealMatrix matrix, int start, int finish, double lamda){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.matrix = matrix;
        this.start = start;
        this.finish = finish;
        this.lamda = lamda;
    }

    public Work(Socket socket, ObjectOutputStream out, ObjectInputStream in, String message, RealMatrix matrix, int start, RealMatrix U, RealMatrix I, double lamda){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.message = message;
        this.matrix = matrix;
        this.start = start;
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
     * Sends matrices to worker after first initialization
     */
    private void sendMatrices() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(matrix);
            out.writeObject(U);
            out.writeObject(I);
            out.writeDouble(lamda);
            out.flush();
            U = (RealMatrix) in.readObject();
            I = (RealMatrix) in.readObject();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * Sends matrices to worker for first initialization
     */
    private void sendInitMatrices() {
        try {
            out.writeObject(message);
            out.flush();
            out.writeObject(matrix);
            out.writeDouble(lamda);
            out.flush();
            U = (RealMatrix) in.readObject();
            I = (RealMatrix) in.readObject();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
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
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
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
     * @return The start of the original matrix (POIS)
     */
    public int getStart(){
        return start;
    }

    /**
     * @return The finish of the original matrix (POIS)
     */
    public int getFinish(){
        return finish;
    }
}
