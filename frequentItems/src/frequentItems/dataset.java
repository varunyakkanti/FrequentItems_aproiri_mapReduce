package frequentItems;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class dataset {
    //C:\Users\varun\Desktop
    public static void main(String args[]) throws IOException
    {
        File file= new File("/home/v/v_yakkan/Desktop/Input.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file,true));
        for(int i=1;i<2000;i++)
        {    writer.write(i+",");
       
            for(int j=0;j<=20;j++)
            {
                writer.write(j+",");
            }
            writer.newLine();
        }
        writer.close();
    }
   
}