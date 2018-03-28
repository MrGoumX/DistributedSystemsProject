package main.cs;

import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MasterNode {

    private List<String[]> lines = new ArrayList<String[]>();
    private Master[] conn;
    private int[] conn_cores;
    private long[] conn_ram;
    private int[] scores;
    private String line, del = ";";
    private BufferedReader br = null;
    private OpenMapRealMatrix POIS;
    private int sol, sor;
    private RealMatrix U, I;

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
            temp += conn_cores[i]*10;
            temp += conn_ram[i]/1073741824;
            scores[i] = temp;
        }
        initialize();
        int sum = 0;
        for(int i = 0; i < args.length; i++){
            sum += scores[i];
        }
        int mo = sum/scores.length;
        int s = POIS.getRowDimension()/mo;
        for(int i = 0; i < args.length; i++){
            int temp = 0;
            int rows = s*scores[i];
            if (i == args.length-1 && rows!=POIS.getRowDimension()){
                rows = POIS.getRowDimension()-1;
            }
            conn[i] = new Master(args[i], "Dist", POIS.getSubMatrix(temp, rows, 0, POIS.getColumnDimension()-1),temp, rows, 0.01);
            temp = rows;
        }
        for(Master i : conn){
            i.start();
        }
        for(Master i : conn){
            i.join();
        }
        for(int i = 0; i < args.length; i++){
            int s1 = conn[i].getStart();
            double[][] Udata = conn[i].getU().getData();
            double[][] Idata = conn[i].getI().getData();
            U.setSubMatrix(Udata, s1, 0);
            I.setSubMatrix(Idata, s1, 0);
        }
        System.out.println(getRecommendation(0,0));
    }

    private void initialize() throws IOException{
        try{
            br = new BufferedReader(new FileReader("C:/Users/MrGoumX/Projects/DistributedSystemsProject/src/main/cs/Dataset1_WZ.csv"));
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
    }

    public int getRecommendation(int row, int col){
        double[][] rec = I.getRowMatrix(row).transpose().multiply(U.getRowMatrix(col)).getData();
        int temp = (int) Math.round(rec[0][0]);
        return temp;
    }
}
