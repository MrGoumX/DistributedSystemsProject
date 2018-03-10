import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Master {
    ServerSocket server;
    Socket dis;
    private int port = 8888;
    public static void main(String args[]){
        new Master().openServer();
    }
    private void openServer() {
        try{
            server = new ServerSocket(port, 3);
            while(true){
                dis = server.accept();
                System.out.println("New info");
                Thread T = new Worker();
                T.start();
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
