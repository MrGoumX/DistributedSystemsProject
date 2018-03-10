import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;

public class Worker extends Thread{

    Worker(){

    }

    public void run(){
        Socket req = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            req = new Socket("localhost", 8888);
            out = new ObjectOutputStream(req.getOutputStream());
            in = new ObjectInputStream(req.getInputStream());




            out.flush();
        }
        catch (UnknownHostException u){
            System.out.println("Unkown host");
        }
        catch(IOException e){
            e.printStackTrace();
        }
        finally {
            try{
                in.close();
                out.close();
                req.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
