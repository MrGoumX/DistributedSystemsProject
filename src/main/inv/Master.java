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

public class Master{

    /**
     * Variable Definition
     */

    // port is the port in which server waits for clients.
    // iterations represents how many times U and I matrices should be trained.
    private final int port, iterations;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private ArrayList<Work> connections = new ArrayList<>(); // connections is a list with Works, each work used by specific worker.
    private ArrayList<Integer> scores = new ArrayList<>(); // list score contains a score about each worker based on number of CPU cores and GB of RAM, so master can distribute properly the work to workers.
    private ArrayList<Integer> rowsf = new ArrayList<>(); // rowsf is a list that contains the limits of rows for each worker to elaborate.
    private ArrayList<Integer> colsf = new ArrayList<>(); // colsf is a list that contains the limits of columns for each worker to elaborate.
    private List<String[]> lines = new ArrayList<>(); // list lines used to read POIS matrix from .csv file.

    // POIS matrix contains csv elements of each point of interest.
    // C matrix contains a score for each point.
    // Bin matrix contains 1 if a user has gone to a point, 0 otherwise.
    private OpenMapRealMatrix POIS, C, Bin;

    // sor = number of values of first csv's line = number of columns = number of points.
    // sol = number of csv file lines = number of rows = number of users.
    // k = factor for U and I one dimension and should be less that max(sor,sol).
    // rr = how many users rows should be elaborated per resource score.
    // rc = how many items(POIS) columns should be elaborated per resource score.
    private int sol, sor, k, rr, rc;

    // U, I are User and Items(POIS) matrices.
    private RealMatrix U, I;


    // lambda is L factor.
    private double err = 0, min = Double.MAX_VALUE, thres = 1, lamda, ferr;

    private String filename; // filename represents path to .csv file, which contains POIS matrix in a specific format.

    /**
     * Constructor
     * @param filename The CSV file to read the POIS matrix off
     * @param iterations The number of iterations of training
     * @param lamda The L factor for the training of the U and I
     * @param ferr The final error needed for the training of U and I
     */
    Master(String filename, int iterations, double lamda, double ferr, int port){
        this.filename = filename;
        this.iterations = iterations;
        this.lamda = lamda;
        this.ferr = ferr;
        this.port = port;
    }

    /**
     * The method that opens the server and differentiate the client from the worker.
     * Also read property of connection and decide if its a client or a worker to do proper actions.
     */
    private void startServer(){
        try{
            initMatrices(filename);
            ServerSocket server = new ServerSocket(port);

            while(true){

                socket = server.accept();
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                out.writeObject("Hello, I'm Master");
                out.flush();
                String diff = (String) in.readObject();
                if(diff.equalsIgnoreCase("Hello, I'm Worker")){
                    worker();
                }
                else if(diff.equalsIgnoreCase("Hello, I'm Client")){
                    client();
                }
            }
        }catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }

    }

    /**
     * The method that manages the communication and work between server and worker
     */
    private void worker(){
        try {
            // creates new work and bind it with its worker through socket.
            Work W = new Work(socket, out, in);
            connections.add(W);

            // for all Works (and so for all workers) get stats.
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

            calcDist(); // calculate proper amount of work per resource(ram and cpu cores).
            calcUI(); // initialization of U, I matrices and k factor.
            calcStarts();
            dist();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * Calculates the distribution based on a scoring system
     */
    private void calcDist() {

        // calculate score of each worker and update score list.
        for(int i = 0; i < connections.size(); i++){
            int score = 0;
            score += connections.get(i).getCores()*2;
            score += connections.get(i).getRam()/1073741824; // 1Gb = 1073741824 bytes.
            scores.add(i, score);
        }

        // calculate total score of all workers.
        int total = 0;
        for(Integer i : scores){
            total += i;
        }

        // rr = how many users rows should be elaborated per resource score.
        // rc = how many items(POIS) columns should be elaborated per resource score.
        rr = sol/total;
        rc = sor/total;
        if(rr == 0) rr = 1; // rr = 0 if sol < total
        if(rc == 0) rc = 1; // rc = 0 if sor < total
    }

    /**
     * Calculates the dimensions of U and I
     */
    private void calcUI() {

        k = (sol*sor)/(sol+sor)+1;
        U = MatrixUtils.createRealMatrix(sol, k);
        I = MatrixUtils.createRealMatrix(sor, k);
    }

    /**
     * The method that calculates how to distribute the matrices
     */
    private void calcStarts() {

        // t and t1 contains the last element's indexes of U and I matrices, which has already elaborated from a worker.
        // so current worker elaborates only elements after indexes t and t1.
        int t = 0;
        int t1 = 0;

        for(int j = 0; j < connections.size(); j++) { // for each worker j
            int rows = t + rr * scores.get(j); // start to elaborate from row t to rr*score(j).
            int cols = t1 + rc * scores.get(j); // start to elaborate from column t1 to rc*score(j).
            if(j != connections.size()-1){ // if current worker isn't the last worker.
                rowsf.add(j, rows);
                colsf.add(j, cols);
            } else{ // else if current worker is the last one should to elaborate all the rest elements.
                rowsf.add(j, POIS.getRowDimension());
                colsf.add(j, POIS.getColumnDimension());
            }

            // update t and t1.
            t = rows;
            t1 = cols;
        }
    }

    /**
     * Matrices distribution
     */
    private void dist() {
        for(int i = 0; i < iterations; i++){ // for each iteration of training
            int br = 0; // br is the index of row of U to start elaboration of each worker.
            int bc = 0; // bc is the index of column of I to start elaboration of each worker.

            System.out.println("The number of workers is: " + connections.size());

            for(int j = 0; j < connections.size(); j++){ // for each worker
                if(i == 0) { // if this is the first iteration then initialize and train U and I.
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "InitDist", POIS, br, rowsf.get(j), bc, colsf.get(j), lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }

                } else{ // if current iteration isn't  first one, just train U and I matrices.
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "TrainU", POIS, br, rowsf.get(j), U, I, lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "TrainI", POIS, bc, colsf.get(j), U, I, lamda));
                    connections.get(j).start();
                    try{
                        connections.get(j).join();
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }

                // update next start index for training of U and I matrices.
                br = rowsf.get(j);
                bc = colsf.get(j);
            }

            RealMatrix UT = MatrixUtils.createRealMatrix(sol, k);
            RealMatrix IT = MatrixUtils.createRealMatrix(sor, k);

            for(int j = 0; j < connections.size(); j++){
                double[][] Udata = connections.get(j).getU();
                double[][] Idata = connections.get(j).getI();
                UT = UT.add(MatrixUtils.createRealMatrix(Udata));
                IT = IT.add(MatrixUtils.createRealMatrix(Idata));
            }

            if(thres < ferr){
                //break;
            }

            // update U and I matrices.
            U = UT;
            I = IT;
        }
        client(); // print recommendations as result.
    }

    /**
     * initMatrices() initializes all matrices.
     */
    private void initMatrices(String filename){
        try {
            String line; // line stores temporary each line of .csv file.
            BufferedReader br = new BufferedReader(new FileReader(filename));
            while ((line = br.readLine()) != null) {
                lines.add(line.split(";")); // lines is a list of arrays. Each String array contains csv values for one line.
            }


            sol = lines.size();
            sor = lines.get(0).length;

            // create and initialize a POIS matrix, which contains csv elements of each point of interest.
            POIS = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    POIS.setEntry(i, j, Integer.parseInt(lines.get(i)[j]));
                }
            }

            // create and initialize a Bin matrix.
            Bin = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    Bin.setEntry(i, j, (POIS.getEntry(i, j) > 0) ? 1 : 0);
                }
            }

            // create and initialize a C matrix.
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
     * The method that produces the recommendation after training.
     * It returns recommendation as an array of integer, which represent number of column of POIS.
     * Parameters row and col are the user position (if suppose that user is at a POI).
     * @param row the row needed
     * @param col the column needed
     * @return a list of pois
     */
    public ArrayList<Integer> getRecommendation(int row, int col){

        double[][] rec = U.getRowMatrix(row).transpose().multiply(I.getRowMatrix(col)).getData();
        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Integer> recom = new ArrayList<>();
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
     * Client WIP
     */
    private void client(){
        System.out.println(getRecommendation(2,2));
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
        new Master("C:/Users/Konstantinos/IdeaProjects/DistributedSystemsProject/src/main/cs/Test.csv", 2, 0.01, 0.001, 4200).startServer();
    }
}
