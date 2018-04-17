/*
* HWInfo retruns information such as RAM and CPU about each worker. This class used by worker sendStats() method.
* */

package main;

import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;

public class HWInfo {

    public static int getCores(){
        return Runtime.getRuntime().availableProcessors();
    }

    public static long getRam(){
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
    }

    public static String getIp(){
        try{
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while(interfaces.hasMoreElements()){
                NetworkInterface i = interfaces.nextElement();
                if(i.isLoopback() || !i.isUp() || i.getDisplayName().contains("VirtualBox")){
                    continue;
                }
                Enumeration<InetAddress> add = i.getInetAddresses();
                while(add.hasMoreElements()){
                    InetAddress a = add.nextElement();
                    if(a instanceof Inet6Address){
                        continue;
                    }
                    return a.getHostAddress();
                }
            }
        }
        catch (SocketException e){
            e.printStackTrace();
        }
        return null;
    }

    public static ArrayList<String> getInfo(){
        ArrayList<String> Info = new ArrayList<String>();
        Info.add(String.valueOf(getCores()));
        Info.add(String.valueOf(getRam()));
        return Info;
    }
}
