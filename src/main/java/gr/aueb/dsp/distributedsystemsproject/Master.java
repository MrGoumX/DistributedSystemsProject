package gr.aueb.dsp.distributedsystemsproject;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.StrictMath.pow;
import static java.lang.System.exit;

public class Master{

    private Scanner scanner = new Scanner(System.in); // Scanner for user input
    private ObjectOutputStream out = null; // Output stream for socket management
    private ObjectInputStream in = null; // Input stream for socket management
    private Socket socket = null; // Socket for server management
    private ArrayList<Work> workers = new ArrayList<>(); // connections is a list with Works, each work used by specific worker.
    private ArrayList<Integer> scores = new ArrayList<>(); // list score contains a score about each worker based on number of CPU cores and GB of RAM, so master can distribute properly the work to workers.
    private ArrayList<Integer> rowsf = new ArrayList<>(); // rowsf is a list that contains the limits of rows for each worker to elaborate.
    private ArrayList<Integer> colsf = new ArrayList<>(); // colsf is a list that contains the limits of columns for each worker to elaborate.
    private ArrayList<Integer> cores = new ArrayList<>(); // cores is a list that contains the CPU cores of each worker as a number
    private ArrayList<Long> ram = new ArrayList<>(); // ram is a list that contains the RAM of each worker as a number
    private ArrayList<Long> times = new ArrayList<>(); // times is a list that contains the time of training of every worker
    private POI[] pois = null;

    private boolean trained = false, accept = true; // trained, boolean that indicates that the matrices have finished training, accept, boolean that indicates that indicates that master accept worker connections
    private final int port; // port is the port in which server waits for clients.
    private int iterations;    // iterations represents how many times U and I matrices should be trained.
    private String filename; // filename represents path to .csv file, which contains POIS matrix in a specific format.

    // lambda is L factor.
    private double thres, lamda, currError;

    // POIS matrix contains csv elements of each point of interest.
    // C matrix contains a score for each point.
    // Bin matrix contains 1 if a user has gone to a point, 0 otherwise.
    private OpenMapRealMatrix POIS, C, Bin;

    // sor = number of values of first csv's line = number of columns = number of points.
    // sol = number of csv file lines = number of rows = number of users.
    // k = factor for U and I one dimension and should be less that max(sor,sol).
    // rr = how many users rows should be elaborated per resource score.
    // rc = how many items(POIS) columns should be elaborated per resource score.
    private int sol, sor, k;

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
    Master(String filename, int iterations, int k, double lamda, double thres, int port, int sol, int sor){
        this.filename = filename;
        this.iterations = iterations;
        this.k = k;
        this.lamda = lamda;
        this.thres = thres;
        this.port = port;
        this.sol = sol;
        this.sor = sor;
    }



    /**
     *  method
     */
    public static void main(String[] args) {
        // CSV File Directory
        String filename = "Data.csv" ;
        String path = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "gr" + File.separator + "aueb" + File.separator + "dsp" + File.separator + "distributedsystemsproject" + File.separator + filename;
        new Master(path, 5, 20, 0.1, 0.5, 4200, -1, -1).start();
    }

    public void start(){

        // desplay menu.
        while(true){
            System.out.println("Please select an option");
            System.out.println("1. Open Server");
            System.out.println("2. Change settings");
            System.out.println("3. Train with workers");
            System.out.println("4. Close Client connections");
            System.out.println("0. Exit");

            String ans = scanner.nextLine(); // read option.

            // open server.
            if(ans.equals("1")){
                Thread serverThread = new Thread(() -> startServer());
                serverThread.start();
                Thread checkingThread = new Thread(() -> checkConnections());
                checkingThread.start();
            }
            // change settings.
            else if(ans.equals("2")){
                String temp;
                while(true){
                    System.out.println("Please give new source csv file as string. If you don't want to change press Enter/Return");
                    temp = scanner.nextLine();
                    if(temp.equals("")){
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
                    else if(temp.equals("")){
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
                    else if(temp.equals("")){
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
                    else if(temp.equals("")){
                        break;
                    }
                    else{
                        System.out.println("Invalid Data. Try Again");
                    }
                }
            }
            // train workers.
            else if(ans.equals("3")){
                if(workers.size()!=0) {
                    accept = false;
                    dist();
                }
                else System.out.println("No workers connected");
            }
            // close client connection.
            else if(ans.equals("4")){
                trained = false;
            }
            // exit.
            else if(ans.equals("0")){
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
     * The method that opens the server and differentiates the client from the worker.
     * Also read property of connection and decide if its a client or a worker to do proper actions.
     */
    private void startServer(){
        try {
            ServerSocket server = new ServerSocket(port);
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
     * While the matrices are not training, master sends null packets to check if the connection to the workers is live, if not it removes them from the pool
     */
    private void checkConnections(){
        while(true) {
            for (int i = 0; i < workers.size(); i++) {
                if(accept) {
                    try{
                        workers.get(i).getOut().writeObject(null);
                        workers.get(i).getOut().flush();
                    }
                    catch (IOException e){
                        System.out.println("Worker Disconnected!");
                        cores.remove(i);
                        ram.remove(i);
                        workers.remove(i);
                        times.remove(i);
                    }
                }
            }
            try{
                Thread.sleep(1000);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * Method that initializes POIS, U & I matrices and reads all the POIs info from the JSON file
     */
    private void initUI(){
        initMatrices(filename); // initialize for first time all matrices.
        // Create matrices U (X) and I (Y) for training
        U = MatrixUtils.createRealMatrix(sol, k);
        I = MatrixUtils.createRealMatrix(sor, k);
        JDKRandomGenerator ran = new JDKRandomGenerator(1);
        // Fill U & I with random values
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
        // Re-initiate array pois that holds the POI Data from the JSON file
        pois = new POI[POIS.getColumnDimension()];
        // Reset arraylist for another training
        scores.clear();
        rowsf.clear();
        colsf.clear();
        IntStream.range(0, times.size()).forEach(i -> times.set(i, (long) 0));
        // Read POI Information from JSON file using JSONParser (com.googlecode.json-simple)
        String j_path = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "gr" + File.separator + "aueb" + File.separator + "dsp" + File.separator + "distributedsystemsproject" + File.separator + "POIs.json";
        JSONParser parser = new JSONParser();
        try{
            // Read the whole object and parse it
            Object obj = parser.parse(new FileReader(j_path));
            JSONObject jsonObject = (JSONObject) obj;
            // For every object of this object get its info
            for(int i = 0; i < POIS.getColumnDimension(); i++){
                // POI ID
                Integer temp_i = i;
                // Get the poi object from the above JSONObject
                JSONObject list = (JSONObject) jsonObject.get(temp_i.toString());
                int id = i;
                // POI UID
                String r_id = (String) list.get("POI");
                // POI Latitude
                double lat = (double) list.get("latidude");
                // POI Longitude
                double lon = (double) list.get("longitude");
                // POI Photo lihk
                String photo = (String) list.get("photos");
                // POI Category
                String cat = (String) list.get("POI_category_id");
                // POI Name
                String name = (String) list.get("POI_name");
                // Create POI Object and add it to the array
                POI temp = new POI(id, r_id, lat, lon, photo, cat, name);
                pois[i] = temp;
            }
        }
        catch (Exception e){
        }
        // Initiate scores arraylist
        for(int i = 0; i < workers.size(); i++){
            scores.add(i, 0);
        }
    }

    /**
     * The method that calculates how to distribute the matrices based on PC Specs (CPU & RAM)
     */
    private void calcStarts(int size) {
        // total stats for all the workers
        int total = 0;
        for(int i = 0; i < size; i++){
            // save the stats for every worker for every core, every gigabyte of ram and every second of training that is needed(for first iteration time is 0)
            scores.set(i, cores.get(i) + Math.round(ram.get(i)/1073741824));
            total += cores.get(i) + Math.round(ram.get(i)/1073741824);
        }
        // t and t1 contains the last element's indexes of U and I matrices, which has already elaborated from a worker.
        // so current worker elaborates only elements after indexes t and t1.
        int t = 0;
        int t1 = 0;

        for(int j = 0; j < size; j++) { // for each worker j
            double give = (float)scores.get(j)/total;
            int gr = (int)Math.round(give*sol);
            int gc = (int)Math.round(give*sor);
            int rows = t + gr; // start to elaborate from row t to rr*score(j).
            int cols = t1 + gc; // start to elaborate from column t1 to rc*score(j).

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
     *  *** BONUS ***
     * The method that calculates how to distribute the matrices based on previous training session based on training time
     */
    private void calcStartsTime(int size){
        // Sum the total time needed from the workers
        long total = times.stream().mapToLong(i -> i.longValue()).sum();
        // Create temporary arrays for keeping the indexes for the sort below
        long[] temp_times = new long[size];
        int[] temp_indexes = new int[size];
        // Initiate the rows and columns number to give
        int[] i_rows = new int[size];
        int[] i_cols = new int[size];
        // Populate arrays
        for(int i = 0; i < size; i++){
            temp_times[i] = times.get(i);
            temp_indexes[i] = i;
        }
        // Sort the arrays based on descending time
        for(int i = 0; i < size-1; i++){
            int m_i = i;
            for(int j = i+1; j < size; j++){
                if(temp_times[j] < temp_times[m_i]) m_i = j;
                long temp = temp_times[m_i];
                int temp2 = temp_indexes[m_i];
                temp_times[m_i] = temp_times[i];
                temp_indexes[m_i] = temp_indexes[i];
                temp_times[i] = temp;
                temp_indexes[i] = temp2;
            }
        }
        // Calculate rows and columns to give on every worker based on its time beginning from the slowest one
        int gr = sol, gc = sor;
        for(int i = 0; i < size; i++){
            // Calculate remaining time from the total time
            double reversed = total - temp_times[i];
            // Calculate score rounded
            double t_score = (reversed/total)/(size-1);
            // Calculate rows and columns to give
            int t_rows = (int) (Math.round(t_score * gr));
            int t_cols = (int) (Math.round(t_score * gc));
            // Save it to the unsorted index
            i_rows[temp_indexes[i]] = t_rows;
            i_cols[temp_indexes[i]] = t_cols;
        }
        // t and t1 contains the last element's indexes of U and I matrices, which has already elaborated from a worker.
        // so current worker elaborates only elements after indexes t and t1.
        int t = 0, t1 = 0;
        for(int i = 0; i < size; i++){
            int rows = t + i_rows[i]; // start to elaborate from row t to rr*score(j).
            int cols = t1 + i_cols[i]; // start to elaborate from column t1 to rc*score(j).
            if(i != size-1){ // if current worker isn't the last worker.
                rowsf.add(i, rows);
                colsf.add(i, cols);
            }
            else { // else if current worker is the last one should to elaborate all the rest elements.
                rowsf.add(i, POIS.getRowDimension());
                colsf.add(i, POIS.getColumnDimension());
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
        //Don't accept incoming workers
        accept = false;
        trained = false;
        //Reinitialize numerous variables for different kind of datasets
        POIS = null;
        Bin = null;
        C = null;
        currError = 0;
        //Initializes all the needed metrics, such as score and matrices U, I for the start of the training
        initUI();
        //Send Bin and C matrices to workers
        for(int i = 0; i < workers.size(); i++){
            workers.set(i, new Work(workers.get(i).getSocket(), workers.get(i).getOut(), workers.get(i).getIn(), "BinC", Bin, C));
        }
        startWork();
        //Calculate starts for each worker
        calcStarts(workers.size());
        for(int i = 0; i < iterations; i++){ // for each iteration of training
            int size = workers.size(); // so if connection list updated at the middle of an iteration, there isn't problem because still used old size.
            System.out.println("Iteration number " + (i+1) +"\nThe number of workers is " + size);

            int br = 0; // br is the index of row of U to start elaboration of each worker.
            int bc = 0; // bc is the index of column of I to start elaboration of each worker.

            // Train U (X)
            for(int j = 0; j < size; j++) { // for each worker
                workers.set(j, new Work(workers.get(j).getSocket(), workers.get(j).getOut(), workers.get(j).getIn(), "TrainU", U, I, br, rowsf.get(j), k, lamda));
                br = rowsf.get(j);
            }
            startWork();
            // Combine U (X)
            combineU();
            // Train I (Y)
            for(int j = 0; j < size; j++) {
                workers.set(j, new Work(workers.get(j).getSocket(), workers.get(j).getOut(), workers.get(j).getIn(), "TrainI", U, I, bc, colsf.get(j), k, lamda));
                bc = colsf.get(j);
            }
            startWork();
            // Combine I (Y)
            combineI();
            // Create trained POIS matrix
            tUI = U.multiply(I.transpose());

            // Get error and compare it to the previous one
            double prevError;
            if(i==0){
                prevError = currError = getError();
            }
            else{
                prevError = currError;
                currError = getError();
                if(prevError - currError < thres) break;
            }

            for(Long l : times){
                System.out.println("Time: " + l);
            }

            // Calculate starts based on time
            long max = times.stream().collect(Collectors.summarizingLong((Long::longValue))).getMax();
            long min = times.stream().collect(Collectors.summarizingLong((Long::longValue))).getMin();
            if((double)min/max < .9){
                calcStartsTime(workers.size());
            }




            System.out.println("Trained with error: " + currError);
        }
        // When matrices have finished training accept more workers and clients
        accept = true;
        trained = true;

        System.out.println("Matrices are finished training");
    }

    /**
     * Combines the matrix U from all the workers
     */
    private void combineU() {
        int br = 0;
        for(int i = 0; i < workers.size(); i++){
            // Get U from worker and create RealMatrix
            double [][] temp = workers.get(i).getU();
            RealMatrix TU = MatrixUtils.createRealMatrix(temp);
            // Replace the rows the worker trained
            IntStream.range(br, rowsf.get(i)).parallel().forEach(j -> U.setRowMatrix(j, TU.getRowMatrix(j)));
            br = rowsf.get(i);
            // Update train time
            times.set(i, workers.get(i).getNanotime());
        }
    }

    /**
     * Combines the matrix I from all the workers
     */
    private void combineI(){
        int bc = 0;
        for(int i = 0; i < workers.size(); i++){
            // Get I from worker and create RealMatrix
            double[][] temp = workers.get(i).getI();
            RealMatrix TI = MatrixUtils.createRealMatrix(temp);
            // Replace the rows the worker trained
            IntStream.range(bc, colsf.get(i)).parallel().forEach(j -> I.setRowMatrix(j, TI.getRowMatrix(j)));
            bc = colsf.get(i);
            // Update train time with U training time
            times.set(i, workers.get(i).getNanotime()+times.get(i));
        }
    }

    /**
     * The method that produces the recommendation after training.
     * It returns recommendation as an array of POI, which represent number of column of POIS.
     * Parameters row and col are the user position (if suppose that user is at a POI).
     * @param row the row needed
     * @param n the column needed
     * @param lat the latitude of the user
     * @param lon the longitude of the user
     * @param radius the radius around the user to search in KM
     * @return a list of pois
     */
    private ArrayList<POI> getRecommendation(int row, int n, double lat, double lon, double radius) {
        //Get user row and copy the pois info
        double[][] user = tUI.getRowMatrix(row).getData(); // tUI is trained row for specific user.
        POI[] poi = pois.clone(); // pois is a copy of poi data.
        //set 0 where the user has been
        for(int i = 0; i < user[0].length; i++){ // if user has gone to this place, set its value to NEGATIVE_INFINITY (guard value) so that it's not gonna be added to the recommendation
            if(Bin.getEntry(row, i)>0){
                user[0][i] = Double.NEGATIVE_INFINITY;
            }
        }
        int size = user[0].length; // how many pois exists.
        //sort the array based on descending values using select sort
        for (int i = 0; i < size-1; i++)
        {
            int max_idx = i; // Max index
            for (int j = i+1; j < size; j++)
                if (user[0][j] > user[0][max_idx])
                    max_idx = j; // if value is larger than the current max then exchange the values and the pois
            double temp = user[0][max_idx];
            POI temp2 = poi[max_idx];
            user[0][max_idx] = user[0][i];
            poi[max_idx] = poi[i];
            user[0][i] = temp;
            poi[i] = temp2;
        }
        // create the recommendation arraylist
        ArrayList<POI> rec = new ArrayList<>();

        // count of pois added to recommendation
        int count = 0;
        // Radius of earth is kilometers
        final int R = 6371;
        // Given radius from user in meters
        double met = radius*1000;
        //add the places where the user might be interested to go and is x kilometers around him using Haversine formula
        for(int i = 0; i < size; i++){
            double latDist = Math.toRadians(poi[i].getLatitude() - lat);
            double lonDist = Math.toRadians(poi[i].getLongitude() - lon);
            double a = Math.sin(latDist / 2) * Math.sin(latDist / 2)
                    + Math.cos(Math.toRadians(poi[i].getLatitude())) * Math.cos(Math.toRadians(lat))
                    * Math.sin(lonDist / 2) * Math.sin(lonDist / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = R * c * 1000;
            distance = Math.pow(distance, 2);
            distance = Math.sqrt(distance);
            // if user hasn't gone and distance is valid then add poi to list.
            if(user[0][i]!=Double.NEGATIVE_INFINITY && distance <= met){
                poi[i].setDistance(distance);
                rec.add(poi[i]);
                count++;
                if(count == n) break; // number of maximum pois defined by user.
            }
        }
        return rec;
    }

    /**
     * @return Error after every iteration of training
     */
    private double getError(){
        double err = 0;
        // base error calculation
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                err += C.getEntry(i,j)*(pow((Bin.getEntry(i,j)-tUI.getEntry(i,j)),2));
            }
        }
        // calculate normalization term
        double normalization = (pow(U.getFrobeniusNorm(), 2) + pow(I.getFrobeniusNorm(), 2))*lamda;
        err += normalization;
        return err;
    }

    /**
     * initMatrices() initializes all matrices.
     */
    private void initMatrices(String filename){
        try {
            String line; // line stores temporary each line of .csv file.
            BufferedReader br = new BufferedReader(new FileReader(filename));
            // If the constructor values are unknown, then find matrix dimensions and read the csv values in POIS matrix
            if (sol < 0 || sor < 0) {
                int max_user = -1;
                int max_poi = -1;
                int size = 0;
                while ((line = br.readLine()) != null) {
                    lines.add(line.split(", ")); // lines is a list of arrays. Each String array contains csv values for one line.
                    int user = Integer.parseInt(lines.get(size)[0]);
                    int poi = Integer.parseInt(lines.get(size)[1]);
                    if (max_user < user) max_user = user;
                    if (max_poi < poi) max_poi = poi;
                    ++size;
                }

                sol = max_user + 1;
                sor = max_poi + 1;

                // create and initialize a POIS matrix, which contains csv elements of each point of interest.
                POIS = new OpenMapRealMatrix(sol, sor);
                // create and initialize a Bin matrix.
                Bin = new OpenMapRealMatrix(sol, sor);
                // create and initialize a C matrix.
                C = new OpenMapRealMatrix(sol, sor);

                // reads the values and builds the Binary matrix
                --size;
                for (; size >= 0; --size) {
                    int user = Integer.parseInt(lines.get(size)[0]);
                    int poi = Integer.parseInt(lines.get(size)[1]);
                    POIS.setEntry(user, poi, Integer.parseInt(lines.get(size)[2]));
                    Bin.setEntry(user, poi, 1);

                }
            }else{
                //if the values of the matrix are given in the constructor then create the matrices immediately and read the values from the CSV file

                // create and initialize a POIS matrix, which contains csv elements of each point of interest.
                POIS = new OpenMapRealMatrix(sol, sor);
                // create and initialize a Bin matrix.
                Bin = new OpenMapRealMatrix(sol, sor);
                // create and initialize a C matrix.
                C = new OpenMapRealMatrix(sol, sor);

                // reads the values and creates and binary matrix
                while ((line = br.readLine()) != null) {
                    String[] data = (line.split(", ")); // lines is a list of arrays. Each String array contains csv values for one line.
                    int user = Integer.parseInt(data[0]);
                    int poi = Integer.parseInt(data[1]);
                    POIS.setEntry(user, poi, Integer.parseInt(data[2]));
                    Bin.setEntry(user, poi, 1);
                }

            }

            // Builds the C matrix based on POIS matrix
            for (int i=0; i<sol; i++){
                for(int j=0; j<sor; j++){
                    C.setEntry(i, j, 1 + 40 * POIS.getEntry(i, j));
                }
            }

        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Worker manager method that gets the stats from the worker and adds its values to specified arraylist
     */
    private void worker(){
        try{
            // creates new work and bind it with its worker through socket.
            Work w = new Work(socket, out, in, "Stats");
            while(true){
                if(!accept){
                    Thread.sleep(2000);
                }
                else{
                    workers.add(w);
                    break;
                }
            }
            w.start(); // read resources from worker
            w.join();
            cores.add(w.getCores()); // Add how many cores to arraylist cores
            ram.add(w.getRam()); // Add how much ram to arraylist ram
            times.add((long) 0); // Initiate train time to zero
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * Client manager method that sends recommendations
     */
    public void client(){
        try {
            int user = in.readInt(); // user id
            int k = in.readInt(); // top k recommendations to send.
            double lat = in.readDouble(); // user latitude
            double lon = in.readDouble(); // user longtitude
            double range = in.readDouble(); // range of recommendation based on location
            out.writeBoolean(trained); // send boolean trained = arrays are ready.
            out.flush();
            if (!trained) {
                out.writeObject("Matrices are not trained yet. So not recommendation for you. For now. OK?");
                out.flush();
            } else { // check if user id is valid.
                if(user < 0 || user > sol-1){
                    out.writeBoolean(false); // Send that the user given is out of bounds
                    out.flush();
                }
                else{
                    out.writeBoolean(true); // Send that the user is valid
                    out.flush();
                    ArrayList<POI> temp = getRecommendation(user,k,lat,lon,range); // Get recommendation arraylist
                    out.writeObject(temp); // Send the recommendation arraylist
                    out.flush();
                }
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
