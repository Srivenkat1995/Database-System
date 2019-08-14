package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;

public class DataObjectIterator implements RowTraverser
{

    String fileName;
    HashMap<String,Integer> fieldMapping;
    ObjectInputStream reader;
    PrimitiveValue[] current;

    DataObjectIterator(String filename,HashMap<String,Integer> fieldMapping)throws IOException
    {
       this.fileName = filename;
       this.fieldMapping = fieldMapping;
       this.reader = new ObjectInputStream(new FileInputStream(filename));
    }
    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        try {
            if (reader != null)
            {
                current =  (PrimitiveValue[]) reader.readObject();

                if (current != null) {
                    return current;
                }
            }
        }
        catch (EOFException e)
        {

        }
        return null;

    }



    @Override
    public void reset() throws IOException, SQLException
    {
        BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(this.fileName));
        this.reader = new ObjectInputStream(buffer);
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping() {
        return null;
    }

    @Override
    public void close() throws IOException
    {
       reader.close();
    }

    @Override
    public PrimitiveValue[] getcurrent()
    {
        return current;
    }

    public void remove()
    {
        File file = new File(fileName);
        file.delete();
    }
}
