package gr.aueb.dsp.distributedsystemsproject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import static java.lang.System.exit;

public class Client extends Thread{
    /**
     * Variable Definition
     */
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int i, j;
    private String message;
    private double lat, lon, rad;

    public Client(String master, int port, int i, int j, double lat, double lon, double rad){
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
        this.i = i;
        this.j = j;
        this.lat = lat;
        this.lon = lon;
        this.rad = rad;
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
                recommendation(); // send message to master that you are worker.
            }
        }
    }

    private void recommendation(){
        try{
            out.writeObject("Hello, I'm Client");
            out.flush();
            out.writeInt(i);
            out.writeInt(j);
            out.writeDouble(lat);
            out.writeDouble(lon);
            out.writeDouble(rad);
            out.flush();
            boolean trained = in.readBoolean();
            if(!trained){
                System.out.println((String) in.readObject());
            }
            else{
                if(in.readBoolean()){
                    int size = in.readInt();
                    ArrayList<POI> temp = new ArrayList<>();
                    for(int i = 0; i < size; i++){
                        int id = in.readInt();
                        String r_id = (String) in.readObject();
                        double lat = in.readDouble();
                        double lng = in.readDouble();
                        String photo = (String) in.readObject();
                        String cat = (String) in.readObject();
                        String name = (String) in.readObject();
                        double distance = in.readDouble();
                        POI t = new POI(id, r_id, lat, lng, photo, cat, name);
                        t.setDistance(distance);
                        temp.add(t);
                    }
                    System.out.println(temp.toString());
                }
                else{
                    System.out.println("User given out of bounds");
                }
            }
            exit(0);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Client("127.0.0.1", 4200, 764, 10, 40.967786, -74.073689683333, 3).start();
    }
}
