package main.inv;

import main.cs.HWInfo;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Worker extends Thread{
    /**
     * Variable Definition
     */
    private ObjectInputStream in;
    private ObjectOutputStream out;

    // message is a command(or an order) from master to worker.
    private String message;

    // ctm is a flag to check if worker communicate with master(ctm=1), and ctm=0 otherwise.
    private boolean ctm = false;

    // sor = number of values of first csv's line = number of columns = number of points.
    // sol = number of csv file lines = number of rows = number of users.
    // k = factor for U and I one dimension and should be less that max(sor,sol).
    // start, finish = limits which specifies a limited area of U and I matrices to train this worker.
    private int sol, sor, k, start, finish;

    // POIS matrix contains csv elements of each point of interest.
    private RealMatrix POIS;

    // C matrix contains a score for each point.
    // Bin matrix contains 1 if a user has gone to a point, 0 otherwise.
    private OpenMapRealMatrix Bin, C;

    // U, I are User and Items(POIS) matrices.
    // orm, ocm, ncm are diagonal matrices with 1 value at diagonal.
    // FS is a temporary matrix for training U and I.
    private RealMatrix U, I, orm, ocm, ncm, FS;

    private double lamda; // lamda is L factor

    /**
     * Constructors
     * parameter master represent ip of master.
     */
    public Worker(String master, int port){

        while(true) {
            try {
                Socket socket = new Socket(master, port);
                if (socket.isConnected()) {
                    out = new ObjectOutputStream(socket.getOutputStream());
                    in = new ObjectInputStream(socket.getInputStream());
                    break;
                }
            } catch (IOException e) { // if connection failed, then wait for 2 seconds and try again.
                System.err.println("Server not live");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    /**
     * Starting method
     */
    public synchronized void run(){
        while(true){
            try{
                message = (String) in.readObject();
            }
            catch (IOException | ClassNotFoundException e){
                e.printStackTrace();
                break;
            }
            if(message.equalsIgnoreCase("Hello, I'm Master")){
                ctm = true;
                init(); // send message to master that you are worker.
            }
            if(ctm && message.equalsIgnoreCase("Stats")){
                sendStats(); // get ram-cores from class HWInfo and send them to master.
            }
            else if(ctm && message.equalsIgnoreCase("InitDist")){
                initTrain(); // manage only the first training of U and I matrices.
            }
            else if(ctm && (message.equalsIgnoreCase("TrainU") || message.equalsIgnoreCase("TrainI"))){
                train(); // manage all trainings(except first) of U and I matrices.
            }
        }
    }

    /**
     * Bind message with server.
     */
    private void init(){
        try{
            out.writeObject("Hello, I'm Worker");
            out.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Sends worker's ram-cores to master.
     */
    private void sendStats(){
        try{
            out.writeObject(HWInfo.getInfo());
            out.flush();
            System.out.println("Sent Stats");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * The method that manages only the 1st training of the matrices. So this method also initialize U and I for first time.
     */
    private void initTrain() {

        try {

            POIS = (RealMatrix) in.readObject();
            lamda = in.readDouble();
            start = in.readInt();
            finish = in.readInt();
            int startI = in.readInt();
            int finishI = in.readInt();
            sol = POIS.getRowDimension();
            sor = POIS.getColumnDimension();
            initUI(); // initialize matrices U and I only for once time.
            initMatrices(); // Initialize Bin, C and diagonal matrices.
            trainU(start, finish);
            trainI(startI, finishI);
            out.writeObject(U);
            out.flush();
            out.writeObject(I);
            out.flush();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * The method that is called once to initialize matrices U, I fot first time.
     */
    private void initUI() {

        k = (sol*sor)/(sol+sor)+1;
        U = MatrixUtils.createRealMatrix(sol, k);
        I = MatrixUtils.createRealMatrix(sor, k);

        // initialize U and I with random values.
        JDKRandomGenerator ran = new JDKRandomGenerator();
        ran.setSeed(1);
        for(int i = 0; i < U.getRowDimension(); i++){
            for(int j = 0; j < U.getColumnDimension(); j++){
                U.setEntry(i, j, (Math.floor(ran.nextFloat()*100)/100));
            }
        }
        for(int i = 0; i < I.getRowDimension(); i++){
            for(int j = 0; j < I.getColumnDimension(); j++){
                I.setEntry(i, j, (Math.floor(ran.nextFloat()*100)/100));
            }
        }

        // add 0.01 to all elements so make sure that U and I don't contains value 0.
        U = U.scalarAdd(0.01);
        I = I.scalarAdd(0.01);
    }

    /**
     * The method that initializes the matrices needed for ALS to work.
     */
    private void initMatrices() {

        // initialize Bin matrix.
        Bin = new OpenMapRealMatrix(sol, sor);
        for (int i = 0; i < sol; i++) {
            for (int j = 0; j < sor; j++) {
                Bin.setEntry(i, j, (POIS.getEntry(i, j) > 0) ? 1 : 0);
            }
        }

        // initialize C matrix.
        C = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                C.setEntry(i, j, 1 + 40*POIS.getEntry(i,j));
            }
        }

        // double arrays or,oc,nc contains 1 values and have sizes of sol, sor and k respectively.
        double[] or = new double[sol];
        double[] oc = new double[sor];
        double[] nc = new double[k];

        for(int i = 0; i < or.length; i++){
            or[i] = 1;
        }
        for(int i = 0; i < oc.length; i++){
            oc[i] = 1;
        }
        for(int i = 0; i < nc.length; i++){
            nc[i] = 1;
        }

        // matrices orm, ocm, ncm are diagonal matrices with 1 values at diagonal.
        orm = MatrixUtils.createRealDiagonalMatrix(or);
        ocm = MatrixUtils.createRealDiagonalMatrix(oc);
        ncm = MatrixUtils.createRealDiagonalMatrix(nc);
    }

    /**
     * The method that trains the Users matrix based on the POI matrix
     */
    private void trainU(int start, int finish){
        RealMatrix IT = I.transpose();
        System.out.println("Rows: ");
        for(int i = start; i < finish; i++) {
            System.out.println(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getRow(i));
            FS = IT.multiply(temp.subtract(ocm));
            FS = FS.multiply(I);
            FS = FS.add(IT.multiply(I));
            FS = FS.add(ncm.scalarMultiply(lamda));
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(IT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(Bin.getRowMatrix(i));
            U.setRowMatrix(i, FS);
        }
    }
    /**
     * The method that trains the POI matrix based on the Users matrix
     */
    private void trainI(int start, int finish) {
        RealMatrix UT = U.transpose();
        System.out.println("Columns: ");
        for(int i = start; i < finish; i++) {
            System.out.println(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getColumn(i));
            FS = UT.multiply(temp.subtract(orm));
            FS = FS.multiply(U);
            FS = FS.add(UT.multiply(U));
            FS = FS.add( ncm.scalarMultiply(lamda));
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(UT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(Bin.getColumnMatrix(i).transpose());
            I.setRowMatrix(i, FS);
        }
    }

    /**
     * The method that manages the training of the matrices after 1st initialization
     */
    private void train() {
        try {
            POIS = (RealMatrix) in.readObject();
            U = (RealMatrix) in.readObject();
            I = (RealMatrix) in.readObject();
            lamda = in.readDouble();
            start = in.readInt();
            finish = in.readInt();
            sol = POIS.getRowDimension();
            sor = POIS.getColumnDimension();
            initMatrices();
            if(message.equalsIgnoreCase("TrainU")){
                trainU(start, finish);
                out.writeObject(U);
                out.flush();
            }
            else if(message.equalsIgnoreCase("TrainI")){
                trainI(start, finish);
                out.writeObject(I);
                out.flush();
            }
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }



    /**
     * Main method
     */
    public static void main(String[] args) {
        new Worker("127.0.0.1", 4200).start();
    }
}
