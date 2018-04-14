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

    private BufferedReader br = null;

    // sor = number of values of first csv's line = number of columns = number of points.
    // sol = number of csv file lines = number of rows = number of users.
    private int sol, sor;

    // C matrix contains a score for each point.
    // Bin matrix contains 1 if a user has gone to a point, 0 otherwise.
    private OpenMapRealMatrix Bin, C;

    // orm, ocm, ncm are diagonal matrices with 1 value at diagonal.
    // U, I are User and Items(POIS) matrices.
    // FS is a temporary matrix for training U and I.
    private RealMatrix U, I, orm, ocm, ncm, FS;

    private double lamda, err = 0; // lamda = λ factor.

    // Constructor
    // file is the .csv file which contains users's check-in.
    ALS_Parted(String file, double lamda){
        this.lamda = lamda;
        try{
            br = new BufferedReader(new FileReader(file)); // initialize br to read from csv file.
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    // initMatrices() initializes all matrices.
    private void initMatrices() throws IOException{
        List<String[]> lines = new ArrayList<>(); // lines is a list of arrays. Each String array contains csv values for one line.
        String line; // line is temporary help variable.
        while((line = br.readLine()) != null) {
            lines.add(line.split(";"));
        }
        sol = lines.size();
        sor = lines.get(0).length;

        // create and initialize a POIS matrix, which contains csv elements of each point of interest.
        OpenMapRealMatrix POIS = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++) {
            for (int j = 0; j < sor; j++) {
                POIS.setEntry(i, j, Integer.parseInt(lines.get(i)[j]));
            }
        }

        // create and initialize a Bin matrix.
        Bin = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                Bin.setEntry(i, j, (POIS.getEntry(i,j) > 0) ? 1 : 0);
            }
        }

        // create and initialize a C matrix.
        C = new OpenMapRealMatrix(sol, sor);
        for(int i = 0; i < sol; i++){
            for(int j = 0; j < sor; j++){
                C.setEntry(i, j, 1 + 40*POIS.getEntry(i,j));
            }
        }

        // create and initialize U, I matrices with random values.
        int k = (sol*sor)/(sol+sor) + 1; // calculation of k factor, which used as dimension to U and I matrices to reduce total size of POIS.
        U = MatrixUtils.createRealMatrix(sol, k);
        I = MatrixUtils.createRealMatrix(sor, k);
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

        // create 3 doubles arrays, with sizes equals to number of users, point and n and initialize them with value 1.
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

        // creation of 3 diagonal matrices with value 1 at diagonal.
        orm = MatrixUtils.createRealDiagonalMatrix(or);
        ocm = MatrixUtils.createRealDiagonalMatrix(oc);
        ncm = MatrixUtils.createRealDiagonalMatrix(nc);

        // add 0.01 to each element(with random value) of U and I, so don't contain zero elements any more.
        U = U.scalarAdd(0.01);
        I = I.scalarAdd(0.01);
    }

    private void trainU(){
        RealMatrix IT = I.transpose();
        for(int i = 0; i < sol; i ++) {
            double[] row = C.getRow(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(row);
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

    private void trainI(){
        RealMatrix UT = U.transpose();
        for(int i = 0; i < sor; i++) {
            double[] col = C.getColumn(i);
            RealMatrix temp = MatrixUtils.createRealDiagonalMatrix(col);
            FS = UT.multiply(temp.subtract(orm));
            FS = FS.multiply(U);
            FS = FS.add(UT.multiply(U));
            FS = FS.add(ncm.scalarMultiply(lamda));
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

    // getRecommendation() returns recommendation as an array of integer, which represent number of column of POIS.
    // row and col are the user position (if suppose that user is at a POI)
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

    public RealMatrix getU() {
        return U;
    }

    public RealMatrix getI() {
        return I;
    }

    public static void main(String[] args) throws IOException{

        String file = "C:/Users/MrGoumX/Projects/DistributedSystemsProject/src/main/cs/Test.csv";
        double thres = 0.005, lamda = 0.01;
        ALS_Parted ALS = new ALS_Parted(file, lamda);
        ALS.initMatrices();

        // first iteration.
        //ALS.trainU();
        //ALS.trainI();
        double prevError, currentError = ALS.getError();

        // rest iterations.
        // foor loop stops when complete all iterations or the difference of error of 2 iterations become less than 0.005.
        for(int i = 0; i < 2; i++){
            ALS.trainU();
            System.out.println(ALS.getU().toString());
            ALS.trainI();
            System.out.println(ALS.getI().toString());
            prevError = currentError;
            currentError = ALS.getError();
            if( prevError-currentError < thres){
                //break;
            }
        }
        System.out.println(ALS.getI().getEntry(0,0));
        ArrayList<Integer> rec = ALS.getRecommendation(2, 2);
        System.out.println("Rec: " + rec);
    }
}
