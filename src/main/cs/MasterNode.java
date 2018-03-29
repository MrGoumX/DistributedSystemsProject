package main.cs;

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
    private int sol, sor, temp;
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
            System.out.println(args[i]);
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
            int temp = 0;
            temp += conn_cores[i]*1;
            temp += conn_ram[i]/1073741824;
            scores[i] = temp;
        }
        initialize();
        int sum = 0;
        for(int i = 0; i < args.length; i++){
            sum += scores[i];
        }
        int fin = POIS.getRowDimension()/sum;
        for(int j = 0; j < args.length; j++){
            int rows = fin*scores[j];

            if (j == args.length-1 && rows!=POIS.getRowDimension()){
                rows = POIS.getRowDimension()-1;
            }
            System.out.println(rows);
            conn[j] = new Master(args[j], "Dist", POIS.getSubMatrix(temp, rows, 0, POIS.getColumnDimension()-1),temp, rows, lamda);
            temp = rows;
        }
        for(int i = 0; i < 500; i++){
            for(Master k : conn){
                k.start();
            }
            for(Master k : conn){
                k.join();
            }
            for(int j = 0; j < args.length; j++){
                int s1 = conn[j].getStart();
                double[][] Udata = conn[j].getU().getData();
                double[][] Idata = conn[j].getI().getData();
                System.out.println(j);
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
            br = new BufferedReader(new FileReader("C:/Users/MrGoumX/IdeaProjects/DistributedSystemsProject/src/main/cs/Test.csv"));
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
        int temp = (int) Math.round(rec[0][0]);
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
