/*
* CSV class create random Test.csv files with random values, which represent POIS array for testing purposes.*/

package main;

import org.apache.commons.math3.random.JDKRandomGenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class CSV {
    public static void main(String[] args) throws IOException{
        BufferedWriter write = null;
        JDKRandomGenerator ran = new JDKRandomGenerator();

        try {
            write = new BufferedWriter(new FileWriter("Test.csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(int i = 0; i < 5; i++){
            for(int j = 0; j < 6; j++){
                int temp = ran.nextInt(10) & Integer.MAX_VALUE;
                if(Math.random()<0.8){
                    temp = 0;
                }
                write.write(temp + (j == 5 ? System.lineSeparator() : ";"));
            }
        }
        write.close();
    }
}
