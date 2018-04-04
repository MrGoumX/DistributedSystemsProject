package main.mf;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static java.lang.StrictMath.pow;

public class ALS_Parted {
    private String file;
    private List<String[]> lines = new ArrayList<String[]>();
    private String line, del = ";";
    private BufferedReader br = null;
    private int sol, sor, soo, n;
    private OpenMapRealMatrix POIS, Bin, C;
    private RealMatrix U, I, orm, ocm, ncm, UT, IT, MI, FS, MU;
    private JDKRandomGenerator ran = null;
    private double[] or, oc, nc, row, col;
    private double lamda, err = 0;

    ALS_Parted(String file, double lamda){
        this.file = file;
        this.lamda = lamda;
        try{
            br = new BufferedReader(new FileReader(file));
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initMatrices() throws IOException{
        while((line = br.readLine()) != null) {
            lines.add(line.split(del));
        }
        sol = lines.size();
        sor = lines.get(0).length;
        POIS = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++) {
            for (int j = 0; j < sor; j++) {
                POIS.setEntry(i, j, Integer.parseInt(lines.get(i)[j]));
            }
        }
        Bin = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                Bin.setEntry(i, j, (POIS.getEntry(i,j) > 0) ? 1 : 0);
            }
        }
        C = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                C.setEntry(i, j, 1 + 40*POIS.getEntry(i,j));
            }
        }
        soo = sol*sor;
        //n = (sol < sor) ? sol : sor;
        n = soo/(sol+sor) + 1;
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
            FS = FS.preMultiply(Bin.getRowMatrix(i));
            U.setRowMatrix(i, FS);
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
            FS = FS.preMultiply(Bin.getColumnMatrix(i).transpose());
            I.setRowMatrix(i, FS);
        }
    }

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

    public RealMatrix getU() {
        return U;
    }

    public RealMatrix getI() {
        return I;
    }

    public static void main(String[] args) throws IOException{
        String file = "C:/Users/Desktop/IdeaProjects/DistributedSystemsProject/src/main/cs/Test.csv";
        double min = Double.MAX_VALUE, thres = 0, lamda = 0.01;
        ALS_Parted ALS = new ALS_Parted(file, lamda);
        ALS.initMatrices();
        for(int i = 0; i < 2; i++){
            ALS.trainU();
            ALS.trainI();
            if(i > 0){
                double temp = min;
                min = ALS.getError();
                thres = min - temp;
            }
            if(thres < 0.005){
                //break;
            }
            //System.out.println("Trained with error " + thres);
            //
        }
        ArrayList<Integer> rec = ALS.getRecommendation(2, 2);
        System.out.println("Rec: " + rec);
    }
}
