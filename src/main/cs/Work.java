package main.cs;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.io.*;
import java.net.Socket;

import static java.lang.StrictMath.pow;

public class Work extends Thread implements Serializable{
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private String message;
    private int sol, sor, soo, n, start, finish;
    private RealMatrix POIS;
    private OpenMapRealMatrix Bin, C;
    private RealMatrix U, I, orm, ocm, ncm, UT, IT, MI, FS, MU;
    private JDKRandomGenerator ran = null;
    private double[] or, oc, nc, row, col;
    private double lamda, err = 0;

    public Work(Socket connection){
        try{
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void run(){
        try {
            String bind = "Hello, I'm a worker";
            out.writeObject(bind);
            out.flush();
            message = (String) in.readObject();
            System.out.println(message);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        if(message.equalsIgnoreCase("Stats")){
            sendStats();
        }
        else if(message.equalsIgnoreCase("Dist")){
            Train();
        }
        else if(message.equalsIgnoreCase("Close")){
            close();
        }
    }

    private void Train() {
        try {
            POIS = (RealMatrix) in.readObject();
            lamda = in.readDouble();
            sol = POIS.getRowDimension();
            sor = POIS.getColumnDimension();
            double min = Double.MAX_VALUE, thres = 0, lamda = 0.01;
            initMatrices();
            trainU();
            trainI();
            out.writeObject(U);
            out.writeObject(I);

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }

    }

    private void sendStats(){
        try{
            out.writeObject(HWInfo.getInfo());
            out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    private void close(){
        try {
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initMatrices() {
        C = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                C.setEntry(i, j, 1 + 40*POIS.getEntry(i,j));
            }
        }
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
        U = U.scalarAdd(0.01);
        I = I.scalarAdd(0.01);
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
            U.setRowMatrix(i, FS);
            System.out.println(i);
        }
    }

    private void trainI(){
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
            System.out.println(i);
        }
    }
}
