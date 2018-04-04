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
    private ArrayList<Integer> rowsf = new ArrayList<Integer>();
    private ArrayList<Integer> colsf = new ArrayList<Integer>();
    private List<String[]> lines = new ArrayList<String[]>();
    private OpenMapRealMatrix POIS, C, Bin;
    private int sol, sor, soo, n, temp, rr, rc, iterations;
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
        //new Thread(()->openServer()).start();
        openServer();
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
                Work T = new Work(connections.get(i).getSocket(), connections.get(i).getOut(), connections.get(i).getIn(), "Stats");
                connections.set(i, T);
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
        int t = 0;
        int t1 = 0;
        for(int j = 0; j < connections.size(); j++) {
            int rows = t + rr * scores.get(j);
            int cols = t1 + rc * scores.get(j);
            if(j != connections.size()-1){
                rowsf.add(j, rows);
                colsf.add(j, cols);
            }
            else{
                if(rows != POIS.getRowDimension()){
                    rowsf.add(j, POIS.getRowDimension());
                }
                if(cols != POIS.getColumnDimension()){
                    colsf.add(j, POIS.getColumnDimension());
                }
            }
            t = rows;
            t1 = cols;
        }
    }

    /**
     * Matrices distribution
     */
    private void dist() {
        for(int i = 0; i < iterations; i++){
            int br = 0;
            int bc = 0;
            System.out.println(connections.size());
            for(int j = 0; j < connections.size(); j++){
                //TODO: WHAT IS WRONG: FOR EACH ITERATION DIFFERENT CONSTRUCTOR FOR TRAINING U AND I BECAUSE IN ORDER TO UPDATE I WE NEED TO HAVE UPDATED U. ALSO BREAKS UP THE WORK
                if(i == 0) {
                    //connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "InitDist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension()-1), br, starts.get(j), lamda));
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "InitDist", POIS, br, rowsf.get(j), bc, colsf.get(j), lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    //startWork(connections.get(j));
                }
                else{
                    //connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "Dist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension() - 1), br, starts.get(j), U.getSubMatrix(br, starts.get(j), 0, U.getColumnDimension() - 1), I.getSubMatrix(br, starts.get(j), 0, I.getColumnDimension() - 1), lamda));
                    //connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "Dist", POIS, br, rowsf.get(j), U, I, lamda));
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "TrainU", POIS, br, rowsf.get(j), U, I, lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    //startWork(connections.get(j));
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "TrainI", POIS, bc, colsf.get(j), U, I, lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    //startWork(connections.get(j));
                }
                br = rowsf.get(j);
                bc = colsf.get(j);
            }
            RealMatrix UT = MatrixUtils.createRealMatrix(sol, n);
            RealMatrix IT = MatrixUtils.createRealMatrix(sor, n);
            for(int j = 0; j < connections.size(); j++){
                //int s1 = connections.get(j).getStart();
                double[][] Udata = connections.get(j).getU();
                double[][] Idata = connections.get(j).getI();
                RealMatrix Utemp = MatrixUtils.createRealMatrix(Udata);
                RealMatrix Itemp = MatrixUtils.createRealMatrix(Idata);
                UT = UT.add(Utemp);
                IT = IT.add(Itemp);
                //IT = IT.add();
            }
            if(i > 0){
                double temp = min;
                min = getError();
                thres = min - temp;
            }
            if(thres < ferr){
                //break;
            }
            U = UT;
            I = IT;
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
            score += connections.get(i).getCores()*2;
            score += connections.get(i).getRam()/1073741824;
            scores.add(i, score);
        }
        int total = 0;
        for(Integer i : scores){
            total += i;
        }
        rr = sol/total;
        rc = sor/total;
        if(rr == 0) rr = 1;
        if(rc == 0) rc = 1;
    }

    private void startWork(Work i){
        try {
            i.start();
            i.join();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
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
        new Master("C:/Users/Desktop/IdeaProjects/DistributedSystemsProject/src/main/cs/Test.csv", 2, 0.01, 0.001).start();
    }
}
