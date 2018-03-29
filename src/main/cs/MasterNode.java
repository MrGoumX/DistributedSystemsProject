package main.cs;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.StrictMath.pow;

public class MasterNode {

    private List<String[]> lines = new ArrayList<String[]>();
    private Master[] conn;
    private int[] conn_cores;
    private long[] conn_ram;
    private int[] scores;
    private String line, del = ";";
    private BufferedReader br = null;
    private OpenMapRealMatrix POIS, C, Bin;
    private int sol, sor, soo, n, temp;
    private RealMatrix U, I;
    private double err = 0, min = Double.MAX_VALUE, thres = 1, lamda = 0.01;

    public static void main(String[] args) throws Exception{
        new MasterNode().run(args);
    }

    public void run(String[] args) throws Exception {
        conn = new Master[args.length];
        conn_cores = new int[args.length];
        conn_ram = new long[args.length];
        scores = new int[args.length];
        for(int i = 0; i < args.length; i++){
            conn[i] = new Master(args[i], "Stats");
        }
        for(Master i : conn){
            i.start();
        }
        for(Thread i : conn){
            i.join();
        }
        for(int i  = 0; i < args.length; i++){
            conn_cores[i] = conn[i].getCores();
            conn_ram[i] = conn[i].getRam();
        }
        for(int i = 0; i < args.length; i++){
            int temp2 = 0;
            temp2 += conn_cores[i]*1;
            temp2 += conn_ram[i]/1073741824;
            scores[i] = temp2;
        }
        initialize();
        int sum = 0;
        for(int i = 0; i < args.length; i++){
            sum += scores[i];
        }
        int mo = sum/scores.length;
        int s = POIS.getRowDimension()/mo;
        soo = sol*sor;
        n = soo/(sol+sor);
        U = MatrixUtils.createRealMatrix(sol, n);
        I = MatrixUtils.createRealMatrix(sor, n);
        for(int i = 0; i < 500; i++){
            for(int j = 0; j < args.length; j++){
                int temp3 = 0;
                int rows = s*scores[j];
                if (j == args.length-1 && rows!=POIS.getRowDimension()){
                    rows = POIS.getRowDimension()-1;
                }
                if(j == 0) {
                    conn[j] = new Master(args[j], "InitDist", POIS.getSubMatrix(temp3, rows, 0, POIS.getColumnDimension() - 1), temp3, rows, lamda);
                }
                else{
                    conn[j] = new Master(args[j], "Dist", temp3, POIS.getSubMatrix(temp, rows, 0, POIS.getColumnDimension() - 1), U, I, lamda);
                }
                temp3 = rows;
            }
            for(Master k : conn){
                k.start();
            }
            for(Master k : conn){
                k.join();
            }
            for(int j = 0; j < args.length; j++){
                int s1 = conn[j].getStart();
                //System.out.println(s1);
                double[][] Udata = conn[j].getU();
                double[][] Idata = conn[j].getI();
                U.setSubMatrix(Udata, s1, 0);
                I.setSubMatrix(Idata, s1, 0);
            }
            if(i > 0){
                double temp = min;
                min = getError();
                thres = min - temp;
            }
            if(thres < 0.005){
                break;
            }
        }
        System.out.println(getRecommendation(0,0));
    }

    private void initialize() throws IOException{
        try{
            br = new BufferedReader(new FileReader("C:/Users/MrGoumX/Projects/DistributedSystemsProject/src/main/cs/Test.csv"));
        }
        catch (IOException e){
            e.printStackTrace();
        }
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
    }

    public int getRecommendation(int row, int col){
        double[][] rec = I.getRowMatrix(row).transpose().multiply(U.getRowMatrix(col)).getData();
        //int temp = (int) Math.round(rec[0][0]);
        return temp;
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
}
