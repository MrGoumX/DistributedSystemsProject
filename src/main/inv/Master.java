package main.inv;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static java.lang.StrictMath.pow;

public class Master extends Thread {
    private ServerSocket server;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private BufferedReader br;
    private ArrayList<Work> connections = new ArrayList<Work>();
    private ArrayList<Integer> scores = new ArrayList<Integer>();
    private ArrayList<Integer> starts = new ArrayList<Integer>();
    private List<String[]> lines = new ArrayList<String[]>();
    private OpenMapRealMatrix POIS, C, Bin;
    private int sol, sor, soo, n, temp, r, iterations;
    private RealMatrix U, I;
    private final int port = 4200;
    private String diff, line, del = ";";
    private final String wi = "Hello, I'm Worker", ci = "Hello, I'm Client";
    private double err = 0, min = Double.MAX_VALUE, thres = 1, lamda, ferr;
    private String filename;

    Master(String filename, int iterations, double lamda, double ferr){
        this.filename = filename;
        this.iterations = iterations;
        this.lamda = lamda;
        this.ferr = ferr;
    }

    public void run(){
        new Thread(()->initMatrices(filename)).start();
        new Thread(()->openServer()).start();
    }

    private void openServer(){
        try{
            server = new ServerSocket(port);
            while(true){
                socket = server.accept();
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                out.writeObject("Hello, I'm Master");
                out.flush();
                diff = (String) in.readObject();
                if(diff.equalsIgnoreCase(wi)){
                    worker();
                }
                else if(diff.equalsIgnoreCase(ci)){
                    client();
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void initMatrices(String filename){
        try {
            br = new BufferedReader(new FileReader(filename));
            while ((line = br.readLine()) != null) {
                lines.add(line.split(del));
            }
            sol = lines.size();
            sor = lines.get(0).length;
            POIS = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    POIS.setEntry(i, j, Integer.parseInt(lines.get(i)[j]));
                }
            }
            Bin = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    Bin.setEntry(i, j, (POIS.getEntry(i, j) > 0) ? 1 : 0);
                }
            }
            C = new OpenMapRealMatrix(sol, sor);
            for (int i = 0; i < sol; i++) {
                for (int j = 0; j < sor; j++) {
                    C.setEntry(i, j, 1 + 40 * POIS.getEntry(i, j));
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void worker(){
        try {
            Work W = new Work(socket, out, in);
            connections.add(W);
            W.start();
            W.join();
            for(int i = 0; i < connections.size(); i++){
                connections.set(i, new Work(connections.get(i).getSocket(), connections.get(i).getOut(), connections.get(i).getIn(), "Stats"));
            }
            for(Work i : connections){
                i.start();
            }
            for(Work i : connections){
                i.join();
            }
            calcDist();
            calcUI();
            calcStarts();
            dist();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    private void calcStarts() {
        for(int j = 0; j < connections.size(); j++) {
            int br = 0;
            int rows = r * scores.get(j);
            if (j == connections.size() - 1 && rows != POIS.getRowDimension()) {
                starts.add(POIS.getRowDimension() - 1);
            }
            br = rows;
        }
    }

    private void dist() {
        for(int i = 0; i < iterations; i++){
            int br = 0;
            for(int j = 0; j < connections.size(); j++){
                if(j == 0) {
                    //conn[j] = new main.cs.Master(args[j], "InitDist", POIS.getSubMatrix(temp3, rows, 0, POIS.getColumnDimension() - 1), temp3, rows, lamda);
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "InitDist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension() - 1), br, starts.get(j), lamda));
                }
                else{
                    //conn[j] = new main.cs.Master(args[j], "Dist", temp3, POIS.getSubMatrix(temp3, rows, 0, POIS.getColumnDimension() - 1), U, I, lamda);
                    connections.set(j, new Work(connections.get(j).getSocket(), connections.get(j).getOut(), connections.get(j).getIn(), "Dist", POIS.getSubMatrix(br, starts.get(j), 0, POIS.getColumnDimension() - 1), br, U, I, lamda));
                }
            }
            for(Work w : connections){
                w.start();
            }
            for(Work w : connections){
                try {
                    w.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for(int j = 0; j < connections.size(); j++){
                int s1 = connections.get(j).getStart();
                double[][] Udata = connections.get(j).getU();
                double[][] Idata = connections.get(j).getI();
                U.setSubMatrix(Udata, s1, 0);
                I.setSubMatrix(Idata, s1, 0);
            }
            if(i > 0){
                double temp = min;
                min = getError();
                thres = min - temp;
            }
            if(thres < ferr){
                break;
            }
        }
        client();
    }

    public double getRecommendation(int row, int col){
        double[][] rec = U.transpose().getRowMatrix(row).multiply(I.getColumnMatrix(col)).getData();
        System.out.println(rec.length);
        double t = rec[0][0];
        return t;
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

    private void calcUI() {
        soo = sol*sor;
        n = soo/(sol+sor);
        U = MatrixUtils.createRealMatrix(sol, n);
        I = MatrixUtils.createRealMatrix(sor, n);

    }

    private void calcDist() {
        for(int i = 0; i < connections.size(); i++){
            int score = 0;
            System.out.println(connections.size());
            score += connections.get(i).getRam()*1;
            score += connections.get(i).getCores()/1073741824;
            scores.add(score);
        }
        int total = 0;
        for(Integer i : scores){
            total += i;
        }
        r = sol/total;
    }

    private void client(){
        System.out.println(getRecommendation(0,0));
        System.out.println(getRecommendation(1,1));
        System.out.println(getRecommendation(2,2));
        System.out.println(getRecommendation(3,3));
        System.out.println(getRecommendation(4,4));
    }

    public static void main(String[] args) {
        new Master("C:/Users/MrGoumX/Projects/DistributedSystemsProject/src/main/cs/Test.csv", 500, 0.01, 0.001).start();
    }
}
