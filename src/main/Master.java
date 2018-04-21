//TODO: CLEAN UP ALL DATA AFTER TRAINING DUE TO RAM ISSUES!!!
package main;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.stream.IntStream;

import static java.lang.StrictMath.pow;
import static java.lang.System.exit;

public class Master{

    private Scanner scanner = new Scanner(System.in); // Scanner for user input
    private ObjectOutputStream out = null; // Output stream for socket management
    private ObjectInputStream in = null; // Input stream for socket management
    private ServerSocket server = null; // ServerSocket for server management
    private Socket socket = null; // Socket for server management
    private ArrayList<Work> workers = new ArrayList<>(); // connections is a list with Works, each work used by specific worker.
    private ArrayList<Integer> scores = new ArrayList<>(); // list score contains a score about each worker based on number of CPU cores and GB of RAM, so master can distribute properly the work to workers.
    private ArrayList<Integer> rowsf = new ArrayList<>(); // rowsf is a list that contains the limits of rows for each worker to elaborate.
    private ArrayList<Integer> colsf = new ArrayList<>(); // colsf is a list that contains the limits of columns for each worker to elaborate.
    private ArrayList<Integer> cores = new ArrayList<>(); // cores is a list that contains the CPU cores of each worker as a number
    private ArrayList<Long> ram = new ArrayList<>(); // ram is a list that contains the RAM of each worker as a number
    private Thread serverThread, checkingThread = null;

    // port is the port in which server waits for clients.
    // iterations represents how many times U and I matrices should be trained.
    private boolean trained = true;
    private final int port;
    private int iterations;
    private String filename, ans, temp; // filename represents path to .csv file, which contains POIS matrix in a specific format.

    // lambda is L factor.
    private double thres, lamda, prevError, currError;

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
    private RealMatrix U, I, tUI;

    private List<String[]> lines = new ArrayList<>(); // list lines used to read POIS matrix from .csv file.

    /**
     * Constructor
     * @param filename The CSV file to read the POIS matrix off
     * @param iterations The number of iterations of training
     * @param lamda The L factor for the training of the U and I
     * @param thres The error threshold needed to stop the training of U and I
     */
    Master(String filename, int iterations, int k, double lamda, double thres, int port){
        this.filename = filename;
        this.iterations = iterations;
        this.k = k;
        this.lamda = lamda;
        this.thres = thres;
        this.port = port;
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
        new Master("D:/MGX/Documents/IdeaProjects/DistributedSystemsProject/src/main/Dataset1_WZ.csv", 5, 20, 0.1, 0.05, 4200).start();
    }

    public void start(){
        while(true){
            System.out.println("Please select an option");
            System.out.println("1. Open Server");
            System.out.println("2. Change settings");
            System.out.println("3. Train with workers");
            System.out.println("4. Close Client connections");
            System.out.println("0. Exit");
            ans = scanner.nextLine();
            if(ans.equalsIgnoreCase("1")){
                serverThread = new Thread(() -> startServer());
                serverThread.start();
                checkingThread = new Thread(() -> checkConnections());
                checkingThread.start();
            }
            else if(ans.equalsIgnoreCase("2")){
                while(true){
                    System.out.println("Please give new source csv file as string. If you don't want to change press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.equalsIgnoreCase("")){
                        break;
                    }
                    else{
                        filename = temp;
                        break;
                    }
                }
                while(true){
                    System.out.println("Please give number of iterations or press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.matches("\\d+")){
                        iterations = Integer.parseInt(temp);
                        break;
                    }
                    else if(temp.equalsIgnoreCase("")){
                        break;
                    }
                    else{
                        System.out.println("Invalid Data. Try Again");
                    }
                }
                while(true){
                    System.out.println("Please give k or press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.matches("\\d+")){
                        k = Integer.parseInt(temp);
                        break;
                    }
                    else if(temp.equalsIgnoreCase("")){
                        break;
                    }
                    else{
                        System.out.println("Invalid Data. Try Again");
                    }
                }
                while(true){
                    System.out.println("Please give lambda or press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.matches("\\d+")){
                        lamda = Double.parseDouble(temp);
                        break;
                    }
                    else if(temp.equalsIgnoreCase("")){
                        break;
                    }
                    else{
                        System.out.println("Invalid Data. Try Again");
                    }
                }
                while(true){
                    System.out.println("Please give final error or press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.matches("\\d+")){
                        thres = Double.parseDouble(temp);
                        break;
                    }
                    else if(temp.equalsIgnoreCase("")){
                        break;
                    }
                    else{
                        System.out.println("Invalid Data. Try Again");
                    }
                }
            }
            else if(ans.equalsIgnoreCase("3")){
                dist();
            }
            else if(ans.equalsIgnoreCase("4")){
                trained = false;
            }
            else if(ans.equalsIgnoreCase("0")){
                for(int i = 0; i < workers.size(); i++){
                    workers.set(i, new Work(workers.get(i).getSocket(), workers.get(i).getOut(), workers.get(i).getIn(), "Close"));
                }
                startWork();
                exit(0);
            }
            else{
                System.out.println("Invalid Option. Try again");
            }
        }
    }

    /**
     * The method that opens the server and differentiate the client from the worker.
     * Also read property of connection and decide if its a client or a worker to do proper actions.
     */
    private void startServer(){
        try {
            server = new ServerSocket(port);
            while (true) {
                socket = server.accept();
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                out.writeObject("Hello, I'm Master");
                out.flush();
                String diff = (String) in.readObject();
                if (diff.equalsIgnoreCase("Hello, I'm Worker")) {
                    worker();
                    System.out.println("New worker connected!");
                } else if (diff.equalsIgnoreCase("Hello, I'm Client")) {
                    client();
                    System.out.println("New client query!");
                }
            }
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * Method that checks all worker connections
     */
    private void checkConnections(){
        while(true){
            for(int i = 0; i < workers.size(); i++){
                if(!workers.get(i).getSocket().isConnected()){
                    System.out.println("Worker Disconnected!");
                    cores.remove(i);
                    ram.remove(i);
                    workers.remove(i);
                }
            }
        }
    }

    /**
     * Method that initializes POIS, U & I matrices
     */
    private void initUI(){
        initMatrices(filename); // initialize for first time all matrices.
        U = MatrixUtils.createRealMatrix(sol, k);
        I = MatrixUtils.createRealMatrix(sor, k);
        JDKRandomGenerator ran = new JDKRandomGenerator(1);
        for(int i = 0; i < U.getRowDimension(); i++){
            for(int j = 0; j < U.getColumnDimension(); j++){
                U.setEntry(i, j, ran.nextDouble());
            }
        }
        for(int i = 0; i < I.getRowDimension(); i++){
            for(int j = 0; j < I.getColumnDimension(); j++){
                I.setEntry(i,j, ran.nextDouble());
            }
        }
        U = U.scalarAdd(0.01);
        I = I.scalarAdd(0.01);
        scores.clear();
        rowsf.clear();
        colsf.clear();
    }

    /**
     * Calculates the distribution based on a scoring system
     */
    private void calcDist(int size) {
        // calculate score of each worker and update score list.
        for(int i = 0; i < size; i++){
            int score = 0;
            score += cores.get(i)*2;
            score += ram.get(i)/1073741824; // 1Gb = 1073741824 bytes.
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
     * The method that calculates how to distribute the matrices
     */
    private void calcStarts(int size) {
        // t and t1 contains the last element's indexes of U and I matrices, which has already elaborated from a worker.
        // so current worker elaborates only elements after indexes t and t1.
        int t = 0;
        int t1 = 0;

        for(int j = 0; j < size; j++) { // for each worker j
            int rows = t + rr * scores.get(j); // start to elaborate from row t to rr*score(j).
            int cols = t1 + rc * scores.get(j); // start to elaborate from column t1 to rc*score(j).
            if(j != size - 1){ // if current worker isn't the last worker.
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
        //Initializes all the needed metrics, such as score and matrices U, I for the start of the training
        trained = false;
        initUI();
        calcDist(workers.size());
        calcStarts(workers.size());

        for(int i = 0; i < workers.size(); i++){
            workers.set(i, new Work(workers.get(i).getSocket(), workers.get(i).getOut(), workers.get(i).getIn(), "BinC", Bin, C));
        }
        startWork();
        for(int i = 0; i < iterations; i++){ // for each iteration of training

            int size = workers.size(); // so if connection list updated at the middle of an iteration, there isn't problem because still used old size.
            System.out.println("Iteration number " + (i+1) +"\nThe number of workers is " + size);

            int br = 0; // br is the index of row of U to start elaboration of each worker.
            int bc = 0; // bc is the index of column of I to start elaboration of each worker.

            for(int j = 0; j < size; j++) { // for each worker
                workers.set(j, new Work(workers.get(j).getSocket(), workers.get(j).getOut(), workers.get(j).getIn(), "TrainU", U, I, br, rowsf.get(j), k, lamda));
                br = rowsf.get(j);
            }
            startWork();
            combineU();
            for(int j = 0; j < size; j++) {
                workers.set(j, new Work(workers.get(j).getSocket(), workers.get(j).getOut(), workers.get(j).getIn(), "TrainI", U, I, bc, colsf.get(j), k, lamda));
                bc = colsf.get(j);
            }
            startWork();
            combineI();
            tUI = U.multiply(I.transpose());
            if(i==0){
                prevError = currError = getError();
            }
            else{
                prevError = currError;
                currError = getError();
                if(prevError - currError< thres) break;
            }
            System.out.println("Trained with error: " + currError);
        }
        trained = true;
        System.out.println("Matrices are finished training");
    }

    /**
     * Combines the matrix U from all the workers
     */
    private void combineU() {
        int br = 0;
        for(int i = 0; i < workers.size(); i++){
            double [][] temp = workers.get(i).getU();
            RealMatrix TU = MatrixUtils.createRealMatrix(temp);
            IntStream.range(br, rowsf.get(i)).parallel().forEach(j -> U.setRowMatrix(j, TU.getRowMatrix(j)));
            br = rowsf.get(i);
        }
    }

    /**
     * Combines the matrix I from all the workers
     */
    private void combineI(){
        int bc = 0;
        for(int i = 0; i < workers.size(); i++){
            double[][] temp = workers.get(i).getI();
            RealMatrix TI = MatrixUtils.createRealMatrix(temp);
            IntStream.range(bc, colsf.get(i)).parallel().forEach(j -> I.setRowMatrix(j, TI.getRowMatrix(j)));
            bc = colsf.get(i);
        }
    }

    /**
     * The method that produces the recommendation after training.
     * It returns recommendation as an array of integer, which represent number of column of POIS.
     * Parameters row and col are the user position (if suppose that user is at a POI).
     * @param row the row needed
     * @param n the column needed
     * @return a list of pois
     */
    private ArrayList<Integer> getRecommendation(int row, int n){
        double[][] user = tUI.getRowMatrix(row).getData();
        int[] pos = new int[user[0].length];
        for(int i = 0; i < user[0].length; i++){
            if(Bin.getEntry(row, i)>0){
                user[0][i] = 0;
            }
            pos[i] = i;
        }
        int size = user[0].length;
        for (int i = 0; i < size-1; i++)
        {
            int min_idx = i;
            for (int j = i+1; j < size; j++)
                if (user[0][j] > user[0][min_idx])
                    min_idx = j;
            double temp = user[0][min_idx];
            int temp2 = pos[min_idx];
            user[0][min_idx] = user[0][i];
            pos[min_idx] = pos[i];
            user[0][i] = temp;
            pos[i] = temp2;
        }
        ArrayList<Integer> rec = new ArrayList<Integer>();
        for(int i = 0; i < n; i++){
            if(user[0][i]!=0) rec.add(pos[i]);
        }
        return rec;
    }

    /**
     * @return Error after every iteration of training
     */
    private double getError(){
        double err = 0;
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                err += C.getEntry(i,j)*(pow((Bin.getEntry(i,j)-tUI.getEntry(i,j)),2));
            }
        }
        err -= lamda*(I.getFrobeniusNorm() + U.getFrobeniusNorm());
        return err;
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
     * Worker manager method
     */
    private void worker(){
        try{
            // creates new work and bind it with its worker through socket.
            Work w = new Work(socket, out, in, "Stats");
            while(true){
                if(!trained){
                    Thread.sleep(2000);
                }
                else{
                    workers.add(w);
                    break;
                }
            }
            w.start(); // read resources from worker
            w.join();
            cores.add(w.getCores());
            ram.add(w.getRam());
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * Client manager method
     */
    public void client(){
        try {
            int i = in.readInt();
            int j = in.readInt();
            out.writeBoolean(trained);
            out.flush();
            if (!trained) {
                out.writeObject("Matrices are not trained yet. So not recommendation for you. For now. OK?");
                out.flush();
            } else {
                out.writeObject(getRecommendation(i,j));
                out.flush();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Method that initiates all the workers to start work
     */
    private void startWork(){
        for(int i = 0; i < workers.size(); i++) {
            workers.get(i).start();
        }
        for(int i = 0; i < workers.size(); i++){
            try {
                workers.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
