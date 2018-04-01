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
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private final int port = 4200;
    private String message, master;
    private final String mi = "Hello, I'm Master";
    private boolean ctm = false;
    private int sol, sor, soo, n, start, finish;
    private RealMatrix POIS;
    private OpenMapRealMatrix Bin, C;
    private RealMatrix U, I, orm, ocm, ncm, UT, IT, MI, FS, MU;
    private JDKRandomGenerator ran = null;
    private double[][] Udata, Idata;
    private double[] or, oc, nc, row, col;
    private double lamda, err = 0;

    public Worker(String master){
        this.master = master;
        while(true) {
            try {
                socket = new Socket(this.master, port);
                if (socket.isConnected()) {
                    out = new ObjectOutputStream(socket.getOutputStream());
                    in = new ObjectInputStream(socket.getInputStream());
                    break;
                }
            } catch (IOException e) {
                System.err.println("Server not live");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public synchronized void run(){
        while(true){
            try{
                message = (String) in.readObject();
            }
            catch (ClassNotFoundException e){
                e.printStackTrace();
                break;
            }
            catch (IOException e){
                e.printStackTrace();
                break;
            }
            if(message.equalsIgnoreCase(mi)){
                ctm = true;
                init();
            }
            if(ctm && message.equalsIgnoreCase("Stats")){
                sendStats();
            }
            else if(ctm && message.equalsIgnoreCase("InitDist")){
                initTrain();
            }
            else if(ctm && message.equalsIgnoreCase("Dist")){
                train();
            }
        }
    }

    private void train() {
        try {
            POIS = (RealMatrix) in.readObject();
            Udata = (double[][]) in.readObject();
            Idata = (double[][]) in.readObject();
            lamda = in.readDouble();
            U = MatrixUtils.createRealMatrix(Udata);
            I = MatrixUtils.createRealMatrix(Idata);
            sol = POIS.getRowDimension();
            sor = POIS.getColumnDimension();
            double min = Double.MAX_VALUE, thres = 0, lamda = 0.01;
            initMatrices();
            trainU();
            trainI();
            out.writeObject(U);
            out.flush();
            out.writeObject(I);
            out.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void initTrain() {
        try {
            POIS = (RealMatrix) in.readObject();
            lamda = in.readDouble();
            sol = POIS.getRowDimension();
            sor = POIS.getColumnDimension();
            double min = Double.MAX_VALUE, thres = 0, lamda = 0.01;
            initUI();
            initMatrices();
            trainU();
            trainI();
            out.writeObject(U);
            out.flush();
            out.writeObject(I);
            out.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void trainU(){
        IT = I.transpose();
        MI = IT.multiply(I);
        for(int i = 0; i < sol; i ++) {
            row = C.getRow(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(row);
            RealMatrix temp2 = temp.subtract(ocm);
            FS = IT.multiply(temp2);
            FS = FS.multiply(I);
            FS = FS.add(MI);
            RealMatrix temp3 = ncm.scalarMultiply(lamda);
            FS = FS.add(temp3);
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(IT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(C.getRowMatrix(i));
            System.out.println(FS.getRowDimension() + "/" + FS.getColumnDimension());
            U.setRowMatrix(i, FS);
        }

    }

    private void trainI() {
        UT = U.transpose();
        MU = UT.multiply(U);
        for(int i = 0; i < sor; i++) {
            col = C.getColumn(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(col);
            RealMatrix temp2 = temp.subtract(orm);
            FS = UT.multiply(temp2);
            FS = FS.multiply(U);
            FS = FS.add(MU);
            RealMatrix temp3 = ncm.scalarMultiply(lamda);
            FS = FS.add(temp3);
            FS = new QRDecomposition(FS).getSolver().getInverse();
            FS = FS.multiply(UT);
            FS = FS.multiply(temp);
            FS = FS.transpose();
            FS = FS.preMultiply(C.getColumnMatrix(i).transpose());
            I.setRowMatrix(i, FS);
        }
    }

    private void initMatrices() {
        C = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                C.setEntry(i, j, 1 + 40*POIS.getEntry(i,j));
            }
        }
        or = new double[sol];
        oc = new double[sor];
        nc = new double[n];
        for(int i = 0; i < or.length; i++){
            or[i] = 1;
        }
        for(int i = 0; i < oc.length; i++){
            oc[i] = 1;
        }
        for(int i = 0; i < nc.length; i++){
            nc[i] = 1;
        }
        orm = MatrixUtils.createRealDiagonalMatrix(or);
        ocm = MatrixUtils.createRealDiagonalMatrix(oc);
        ncm = MatrixUtils.createRealDiagonalMatrix(nc);
    }

    private void initUI() {
        soo = sol*sor;
        n = soo/(sol+sor);
        U = MatrixUtils.createRealMatrix(sol, n);
        I = MatrixUtils.createRealMatrix(sor, n);
        ran = new JDKRandomGenerator();
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
        U = U.scalarAdd(0.01);
        I = I.scalarAdd(0.01);
    }

    private void init(){
        try{
            out.writeObject("Hello, I'm Worker");
            out.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

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

    public static void main(String[] args) {
        new Worker("127.0.0.1").start();
    }
}
