package main.inv;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static java.lang.StrictMath.pow;

public class Master extends Thread {
    /**
     * Variable Definition
     */
    private ServerSocket server;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private BufferedReader br;
    private ArrayList<Work> connections = new ArrayList<Work>();
    private ArrayList<Integer> scores = new ArrayList<Integer>();
    private ArrayList<Integer> starts = new ArrayList<Integer>();
    private List<String[]> lines = new ArrayList<String[]>();
    private OpenMapRealMatrix POIS, C, Bin;
    private int sol, sor, soo, n, temp, r, iterations;
    private RealMatrix U, I;
    private final int port = 4200;
    private String diff, line, del = ";";
    private final String wi = "Hello, I'm Worker", ci = "Hello, I'm Client";
    private double err = 0, min = Double.MAX_VALUE, thres = 1, lamda, ferr;
    private String filename;

    /**
     * Constructor
     * @param filename The CSV file to read the POIS matrix off
     * @param iterations The number of iterations of training
     * @param lamda The lambda for the training of the U and I
     * @param ferr The final error needed for the training of U and I
     */
    Master(String filename, int iterations, double lamda, double ferr){
        this.filename = filename;
        this.iterations = iterations;
        this.lamda = lamda;
        this.ferr = ferr;
    }

    /**
     * Starting method
     */
    public void run(){
        new Thread(()->initMatrices(filename)).start();
        new Thread(()->openServer()).start();
    }

    /**
     * The method that opens the server and differentiate the client from the worker
     */
    private void openServer(){
        try{
            server = new ServerSocket(port);
            while(true){
                socket = server.accept();
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                out.writeObject("Hello, I'm Master");
                out.flush();
                diff = (String) in.readObject();
                if(diff.equalsIgnoreCase(wi)){
                    worker();
                }
                else if(diff.equalsIgnoreCase(ci)){
                    client();
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * POIS matrix read off
     */
    private void initMatrices(String filename){
        try {
            br = new BufferedReader(new FileReader(filename));
            while ((line = br.readLine()) != null) {
                lines.add(line.split(del));
            }
            sol = lines.size();
            sor = lines.get(0).length;
            POIS = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    POIS.setEntry(i, j, Integer.parseInt(lines.get(i)[j]));
                }
            }
            Bin = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    Bin.setEntry(i, j, (POIS.getEntry(i, j) > 0) ? 1 : 0);
                }
            }
            C = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    C.setEntry(i, j, 1 + 40 * POIS.getEntry(i, j));
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * The method that manages the communication and work between server and worker
     */
    private void worker(){
        try {
            Work W = new Work(socket, out, in);
            connections.add(W);
            W.start();
            W.join();
            for(int i = 0; i < connections.size(); i++){
                connections.set(i, new Work(connections.get(i).getSocket(), connections.get(i).getOut(), connections.get(i).getIn(), "Stats"));
            }
            for(Work i : connections){
                i.start();
            }
            for(Work i : connections){
                i.join();
            }
            calcDist();
            calcUI();
            calcStarts();
            dist();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * The method that calculates how to distribute the matrices
     */
    private void calcStarts() {
        int br = 0;
        for(int j = 0; j < connections.size(); j++) {
            int rows = br + r * scores.get(j);
            starts.add(j, rows);
            System.out.println(rows);
            if (j == connections.size() - 1 && rows != POIS.getRowDimension()) {
                starts.add(j, POIS.getRowDimension() - 1);
            }
            br = rows;
        }
    }

    /**
     * Matrices distribution
     */
    private void dist() {
        for(int i = 0; i < iterations; i++){
            int br = 0;
            for(int j = 0; j < connections.size(); j++){
                if(j == 0) {
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "InitDist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension() - 1), br, starts.get(j), lamda));
                }
                else{
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "Dist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension() - 1), br, U, I, lamda));
                }
            }
            for(Work w : connections){
                w.start();
            }
            for(Work w : connections){
                try {
                    w.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for(int j = 0; j < connections.size(); j++){
                int s1 = connections.get(j).getStart();
                double[][] Udata = connections.get(j).getU();
                double[][] Idata = connections.get(j).getI();
                U.setSubMatrix(Udata, s1, 0);
                I.setSubMatrix(Idata, s1, 0);
            }
            if(i > 0){
                double temp = min;
                min = getError();
                thres = min - temp;
            }
            if(thres < ferr){
                break;
            }
        }
        client();
    }

    /**
     * The method that produces the recommendation after training
     * @param row the row needed
     * @param col the column needed
     * @return a list of pois
     */
    public ArrayList<Integer> getRecommendation(int row, int col){
        double[][] rec = U.getRowMatrix(row).transpose().multiply(I.getRowMatrix(col)).getData();
        ArrayList<Double> values = new ArrayList<Double>();
        ArrayList<Integer> recom = new ArrayList<Integer>();
        for(int i = row; i < row+rec.length; i++){
            if(i < Bin.getRowDimension()) {
                double max = 0;
                int pos = 0;
                for (int j = col; j < col + rec[i - row].length; j++) {
                    if (j < Bin.getColumnDimension()) {
                        if (rec[i - row][j - col] >= max && Bin.getEntry(i, j) == 0) {
                            max = rec[i - row][j - col];
                            pos = j;
                        }
                    }
                }
                if(!recom.contains(pos)) {
                    values.add(max);
                    recom.add(pos);
                }
            }
        }
        return recom;
    }

    /**
     * @return Error after every iteration of training
     */
    private double getError(){
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                double[][] temp = (I.getRowMatrix(i).multiply(U.getRowMatrix(i).transpose())).getData();
                err += C.getEntry(i,j)*(pow((Bin.getEntry(i,j)-temp[0][0]),2));
            }
        }
        err -= lamda*(I.getFrobeniusNorm() + U.getFrobeniusNorm());
        return err;
    }

    /**
     * Calculates the dimensions of U and I
     */
    private void calcUI() {
        soo = sol*sor;
        n = soo/(sol+sor)+1;
        U = MatrixUtils.createRealMatrix(sol, n);
        I = MatrixUtils.createRealMatrix(sor, n);
    }

    /**
     * Calculates the distribution based on a scoring system
     */
    private void calcDist() {
        for(int i = 0; i < connections.size(); i++){
            int score = 0;
            score += connections.get(i).getCores()*1;
            score += connections.get(i).getRam()/1073741824;
            scores.add(score);
        }
        int total = 0;
        for(Integer i : scores){
            total += i;
        }
        r = sol/total;
        if(r == 0) r = 1;
    }

    /**
     * Client WIP
     */
    private void client(){
        System.out.println(getRecommendation(2,2));
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
        new Master("C:/Users/MrGoumX/Projects/DistributedSystemsProject/src/main/cs/Test.csv", 200, 0.01, 0.001).start();
    }
}
