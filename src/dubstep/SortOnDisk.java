package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

public class SortOnDisk implements RowTraverser
{
    private long FileStart;
    RowTraverser rowIterator;
    List<Field> sortFieldList;
    List<OrderByField> orderByFields;
    HashMap<String, Integer> fieldMapping;
    int No_ofFiles;
    private static int BLOCK_SIZE = 1500;
    PrimitiveValue[] current;
    RowTraverser sortedIterator;
    String FileName;
    boolean isInitialized;
    boolean isOderby;
    FieldType []fieldTypes;

    public SortOnDisk(RowTraverser rowIterator, List<Field> sortFieldList,String fileName) throws SQLException,IOException,ClassNotFoundException
    {
        this.No_ofFiles =0;
        this.rowIterator = rowIterator;
        this.sortFieldList = sortFieldList;
        this.fieldMapping = rowIterator.getFieldPositionMapping();
        this.FileStart = Utility.getFileStart();
        this.isInitialized = false;
        this.isOderby = false;
        setFileNames(fileName);
        sort();
        if(No_ofFiles >1) merge();
        setSortedIterator();

    }

    public SortOnDisk(RowTraverser rowIterator, List<OrderByField> orderBy, String fileName, boolean isOrderBy)throws SQLException,IOException,ClassNotFoundException
    {
        this.No_ofFiles =0;
        this.rowIterator = rowIterator;
        this.orderByFields = orderBy;
        this.fieldMapping = rowIterator.getFieldPositionMapping();
        this.FileStart = Utility.getFileStart();
        this.isInitialized = false;
        this.isOderby = isOrderBy;
        setFileNames(fileName);
        sort();
        if(No_ofFiles >1) merge();
        setSortedIterator();


    }



    private void sort() throws SQLException, IOException,ClassNotFoundException
    {
        int fileCount = 1;
        ArrayList<PrimitiveValue[]> dataRows = getNextNrows();

        if(dataRows !=null)
        {
            this.fieldTypes = Utility.getFieldTypes(dataRows.get(0));

            while (dataRows != null && !dataRows.isEmpty())
            {
                if (isOderby)
                {
                    Collections.sort(dataRows, new DataExpressionComparator(orderByFields, fieldMapping));
                }
                else {

                    Collections.sort(dataRows, new DataRowComparator(sortFieldList));

                }

                BufferedWriter out = new BufferedWriter(new FileWriter(FileName + (FileStart + fileCount) + ".csv"));
                out.write(Utility.getLines(dataRows));
                out.flush();
                out.close();
                dataRows = getNextNrows();
                fileCount = fileCount + 1;

            }

            this.No_ofFiles = fileCount - 1;
        }
        rowIterator.close();
    }

    private void setFileNames(String fileName)
    {
        String directory = "MasterMinds/";
        File dir = new File(directory);
        if(!dir.exists())
        {
            dir.mkdir();
        }
        this.FileName =directory+fileName+"-";
    }


    private void merge() throws IOException, SQLException,ClassNotFoundException
    {
        HashMap<Integer,RowTraverser> fileIterators = getFileIterators();
        PriorityQueue<DataRow> priorityQueue = null;
        if(isOderby)
        {
            DataFileExpressionComparator comparator = new DataFileExpressionComparator(orderByFields,fieldMapping);
            priorityQueue = new PriorityQueue<>(fileIterators.size(),comparator);

        }
        else
        {
            DataFileRowComparator comparator = new DataFileRowComparator(sortFieldList);
            priorityQueue = new PriorityQueue<>(fileIterators.size(),comparator);

        }
        priorityQueue = init(priorityQueue, fileIterators);
        BufferedWriter out = new BufferedWriter(new FileWriter(FileName+FileStart+"_final.csv"));
        DataRow dataRow = priorityQueue.poll();
        String Line = "";
        int count = 0;

        while (!fileIterators.isEmpty() && dataRow !=null)
        {
           int file = dataRow.getFileNo();
           out.write(Utility.getLine(dataRow.getDataRow()));
            if(fileIterators.containsKey(file))
            {
               RowTraverser rowIterator = fileIterators.get(file);

                   PrimitiveValue[] data = rowIterator.next();
                   if(data !=null)
                   {
                       priorityQueue.add(new DataRow(file, data));

                   }
               else
               {
                   rowIterator.close();
                   fileIterators.remove(file);

               }
            }
            dataRow = priorityQueue.poll();
        }

        if(!Line.equals(""))
        {
            out.write(Line);
        }

        out.close();

    }
    private PriorityQueue<DataRow> init(PriorityQueue<DataRow> priorityQueue,HashMap<Integer,RowTraverser> fileIterators)throws IOException,SQLException,ClassNotFoundException
    {
        for(int i=1 ; i <=No_ofFiles; i++)
        {
            RowTraverser rowIterator = fileIterators.get(i);
            priorityQueue.add(new DataRow(i, rowIterator.next()));
        }

        return priorityQueue;
    }

    private HashMap<Integer,RowTraverser> getFileIterators() throws IOException
    {
        HashMap<Integer, RowTraverser> fileIterators = new HashMap<>();

        for (int i=1; i <= No_ofFiles; i++)
        {
            RowTraverser rowIterator = new RowIterator(FileName+ (FileStart +i)+ ".csv" ,fieldMapping,fieldTypes);
            fileIterators.put(i,rowIterator);
        }
        return fileIterators;
    }

    private ArrayList<PrimitiveValue[]> getNextNrows()throws SQLException,IOException,ClassNotFoundException
    {
        ArrayList<PrimitiveValue[]> dataRows = new ArrayList<>();

        PrimitiveValue[] dataRow = rowIterator!=null? rowIterator.next(): null;

        if(dataRow !=null)
        {
            int count = 1;

            while (dataRow != null && count <= BLOCK_SIZE)
            {
                dataRows.add(dataRow);
                dataRow = rowIterator.next();
                count = count + 1;
            }
            return dataRows;
        }

        return null;

    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(sortedIterator !=null)
        {
            PrimitiveValue[] dataRow = this.sortedIterator.next();
            if(dataRow !=null)
            {
                return  dataRow;

            }
        }

        return null;

    }



    @Override
    public PrimitiveValue[] getcurrent()
    {
        return current;
    }

    @Override
    public void reset() throws IOException, SQLException,ClassNotFoundException
    {
        setSortedIterator();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return fieldMapping;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
        rowIterator.close();
    }

    public RowTraverser getSortedIterator()
    {
        return sortedIterator;
    }

    public void setSortedIterator() throws IOException,SQLException,ClassNotFoundException
    {
        if(No_ofFiles==0)
        {
            sortedIterator = null;
        }
        else
        {
            if(No_ofFiles >1)
            {
                this.sortedIterator = new RowIterator(FileName+FileStart+"_final.csv",fieldMapping,fieldTypes);
            }
            else
            {
                this.sortedIterator = new RowIterator(FileName + (FileStart +1)+".csv",fieldMapping,fieldTypes);
            }

        }

    }


}
