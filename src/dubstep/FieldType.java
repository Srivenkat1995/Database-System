package dubstep;


import net.sf.jsqlparser.expression.*;

public enum FieldType
{

    STRING(101),DATE(102),INT(103),DOUBLE(104);

    int fieldType;

    FieldType(int type)
    {
        fieldType = type;
    }

    public static FieldType getFieldType(String type)
    {

        type = type.toLowerCase();

        switch(type)
        {
            case "int": return FieldType.INT;
            case "string": return FieldType.STRING;
            case "char": return FieldType.STRING;
            case "varchar": return FieldType.STRING;
            case "date": return FieldType.DATE;
            case "decimal": return FieldType.DOUBLE;
            default: return FieldType.STRING;
        }
    }

    public static PrimitiveValue getPrimitiveValue(String value, FieldType fieldType)
    {
        switch (fieldType)
        {
            case STRING: return new StringValue(value);
            case INT: return new LongValue(value);
            case DATE: return new DateValue(value);
            case DOUBLE: return new DoubleValue(value);
            default: return new StringValue(value);
        }

    }

    public static FieldType getFieldType(PrimitiveValue value)
    {
        if(value instanceof LongValue)
        {
            return FieldType.INT;
        }
        if(value instanceof DoubleValue)
        {
            return FieldType.DOUBLE;
        }
        if(value instanceof  StringValue)
        {
            return FieldType.STRING;
        }
        if(value instanceof DateValue)
        {
            return FieldType.DATE;
        }

        return FieldType.STRING;
    }

    public static boolean isPrimitveValue(Expression value)
    {
        if(value instanceof LongValue)
        {
            return  true;
        }
        if(value instanceof DoubleValue)
        {
            return true;
        }
         if(value instanceof  StringValue)
        {
            return true;
        }
         if(value instanceof DateValue)
         {
             return true;
         }

         return false;


    }
}
