package main;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

import static java.lang.System.exit;


public class Worker extends Thread{
    /**
     * Variable Definition
     */
    private Socket socket;
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

    // C matrix contains a score for each point.
    // Bin matrix contains 1 if a user has gone to a point, 0 otherwise.
    private OpenMapRealMatrix Bin, C;

    // U, I are User and Items(POIS) matrices.
    // orm, ocm, ncm are diagonal matrices with 1 value at diagonal.
    // FS is a temporary matrix for training U and I.
    private RealMatrix U, I, orm, ocm, ncm;

    private double lamda; // lamda is L factor


    /**
     * Constructors
     * parameter master represent ip of master.
     */
    public Worker(String master, int port){
        while(true) {
            try {
                socket = new Socket(master, port);
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
                if(message==null) continue;
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
            else if(ctm && message.equalsIgnoreCase("BinC")){
                receiveBinC();
            }
            else if(ctm && (message.equalsIgnoreCase("TrainU") || message.equalsIgnoreCase("TrainI"))){
                train(); // manage all trainings(except first) of U and I matrices.
            }
            else if(ctm && (message.equalsIgnoreCase("Close"))){
                exit(0);
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
            out.writeObject(getStats());
            out.flush();
            System.out.println("Sent Stats");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Receives matrices Bin & C from Master
     */
    private void receiveBinC() {
        try{
            Bin = (OpenMapRealMatrix) in.readObject();
            C = (OpenMapRealMatrix) in.readObject();
        }
        catch (ClassNotFoundException | IOException e){
            e.printStackTrace();
        }
    }

    /**
     * The method that manages the training of the matrices after 1st initialization
     */
    private void train() {
        try {
            double[][] TU = (double[][]) in.readObject();
            double[][] TI = (double[][]) in.readObject();
            start = in.readInt();
            finish = in.readInt();
            k = in.readInt();
            lamda = in.readDouble();
            U = MatrixUtils.createRealMatrix(TU);
            I = MatrixUtils.createRealMatrix(TI);
            sol = Bin.getRowDimension();
            sor = Bin.getColumnDimension();
            initMatrices();
            if (message.equalsIgnoreCase("TrainU")) {
                initU(start, finish);
                out.writeObject(U.getData());
                out.flush();
            } else if (message.equalsIgnoreCase("TrainI")) {
                initI(start, finish);
                out.writeObject(I.getData());
                out.flush();
            }
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    /**
     * The method that initializes the matrices needed for ALS to work.
     */
    private void initMatrices() {
        // double arrays or,oc,nc contains 1 values and have sizes of sol, sor and k respectively.
        double[] or = new double[sol];
        double[] oc = new double[sor];
        double[] nc = new double[k];

        Arrays.fill(or, 1);
        Arrays.fill(oc, 1);
        Arrays.fill(nc, 1);

        // matrices orm, ocm, ncm are diagonal matrices with 1 values at diagonal.
        orm = MatrixUtils.createRealDiagonalMatrix(or);
        ocm = MatrixUtils.createRealDiagonalMatrix(oc);
        ncm = MatrixUtils.createRealDiagonalMatrix(nc);
    }

    private void initU(int start, int finish){
        System.out.println("Rows: " + start + "-" + finish);
        RealMatrix IT = I.transpose();
        RealMatrix MI = IT.multiply(I);
        IntStream.range(start, finish).parallel().forEach(i -> trainU(i, IT, MI));
        /*for(int i = start; i < finish; i++){
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getRow(i));
            RealMatrix FS = IT.multiply(temp.subtract(ocm));
            FS = FS.multiply(I);
            FS = FS.add(MI);
            FS = FS.add(ncm.scalarMultiply(lamda));
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(IT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(Bin.getRowMatrix(i));
            U.setRowMatrix(i, FS);
        }*/
    }

    /**
     * The method that trains the Users matrix based on the POI matrix
     */
    private void trainU(int i, RealMatrix IT, RealMatrix MI){
        RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getRow(i));
        RealMatrix FS = IT.multiply(temp.subtract(ocm));
        FS = FS.multiply(I);
        FS = FS.add(MI);
        FS = FS.add(ncm.scalarMultiply(lamda));
        FS = new QRDecomposition(FS).getSolver().getInverse();
        FS = FS.multiply(IT);
        FS = FS.multiply(temp);
        FS = FS.transpose();
        FS = FS.preMultiply(Bin.getRowMatrix(i));
        U.setRowMatrix(i, FS);
    }

    private void initI(int start, int finish){
        System.out.println("Columns: " + start + "-" + finish);
        RealMatrix UT = U.transpose();
        RealMatrix MU = UT.multiply(U);
        IntStream.range(start, finish).parallel().forEach(i -> trainI(i, UT, MU));
        /*for(int i = start; i < finish; i++){
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getColumn(i));
            RealMatrix FS = UT.multiply(temp.subtract(orm));
            FS = FS.multiply(U);
            FS = FS.add(MU);
            FS = FS.add(ncm.scalarMultiply(lamda));
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(UT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(Bin.getColumnMatrix(i).transpose());
            I.setRowMatrix(i, FS);
        }*/
    }

    /**
     * The method that trains the POI matrix based on the Users matrix
     */
    private void trainI(int i, RealMatrix UT, RealMatrix MU){
        RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(C.getColumn(i));
        RealMatrix FS = UT.multiply(temp.subtract(orm));
        FS = FS.multiply(U);
        FS = FS.add(MU);
        FS = FS.add(ncm.scalarMultiply(lamda));
        FS = new QRDecomposition(FS).getSolver().getInverse();
        FS = FS.multiply(UT);
        FS = FS.multiply(temp);
        FS = FS.transpose();
        FS = FS.preMultiply(Bin.getColumnMatrix(i).transpose());
        I.setRowMatrix(i, FS);
    }

    private ArrayList<String> getStats(){
        ArrayList<String> stats = new ArrayList<>();
        stats.add(String.valueOf(Runtime.getRuntime().availableProcessors()));
        stats.add(String.valueOf(((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize()));
        return stats;
    }
    /**
     * Main method
     */
    public static void main(String[] args) {
        new Worker("127.0.0.1", 4200).start();
    }
}
