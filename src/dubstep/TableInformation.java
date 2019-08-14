package dubstep;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;

public class TableInformation
{

    static HashMap<String, CreateTable> TableInfo;
    static HashMap<String,List<Field>> TableFieldMappingInfo;

    public static void addTableInfo(String TableName, CreateTable statement)
    {
        if(TableInfo == null)
        {
            TableInfo = new HashMap<String, CreateTable>();
            TableFieldMappingInfo = new HashMap<String,List<Field>>();
        }

        TableInfo.put(TableName,statement);
        addTableFieldMappingInfo(TableName,statement);

    }

    private static void addTableFieldMappingInfo(String TableName, CreateTable statement)
    {
        List<ColumnDefinition> columnDefinitionList = statement.getColumnDefinitions();
        List<Field> FieldsList = new ArrayList<>();

        for(int i =0; i < columnDefinitionList.size(); i++)
        {
           ColumnDefinition columnDef =  columnDefinitionList.get(i);
           String FieldName = columnDef.getColumnName();
           FieldType fieldType = FieldType.getFieldType(columnDef.getColDataType().toString());
           FieldsList.add(new Field(FieldName,i,fieldType));
        }
        TableFieldMappingInfo.put(TableName,FieldsList);
    }

    public static List<Field> getTableFieldMappingInfo(String TableName)
    {
        return TableFieldMappingInfo.get(TableName);
    }




    public static HashMap<String,Integer> getFieldMappingwithAlias(String TableName,String Alias)
    {
        List<Field> Fieldmapping = TableInformation.getTableFieldMappingInfo(TableName);

        HashMap<String,Integer> FieldMappingWithAlias = new HashMap<String,Integer>();

        for(int i=0; i < Fieldmapping.size(); i++)
        {
            String FieldName = Fieldmapping.get(i).getColumnName();
            FieldMappingWithAlias.put((Alias + "." + FieldName), i);
        }

        return FieldMappingWithAlias;
    }


    public static boolean hasTable(String TableName)
    {
        if(TableInfo.get(TableName)!=null )
        {
            return true;
        }
        else {

            return false;
        }
    }

    public static int getMaxPosition(HashMap<String,Integer> positionMapping)
    {
        Iterator<String> Fields = positionMapping.keySet().iterator();
        int max =-1;

        while(Fields.hasNext())
        {
            String key = Fields.next();
            int position = positionMapping.get(key);
            if(position > max)
            {
                max = position;
            }
        }

        return max;
    }




}
