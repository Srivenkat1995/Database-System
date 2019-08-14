package dubstep;

import java.io.*;
import java.nio.Buffer;
import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserTokenManager;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import javax.swing.plaf.basic.BasicButtonUI;
import javax.swing.text.html.HTMLDocument;

public class Main
{

    public static Statement statement;

    public static void main(String args[]) throws ParseException, IOException, SQLException, ClassNotFoundException,CloneNotSupportedException, IllegalAccessException, InstantiationException
    {
        CCJSqlParser parser = new CCJSqlParser(System.in);
        boolean inMemory = true;
        FileWriter csvWriter = null;
        List<String> updateQueries = new LinkedList<>();
        HashMap<String,List<Expression>> deleteExpression = new HashMap<>();
        List<Expression> deleteExpressions = new ArrayList<>();
        if(args.length > 0 && args[0].equals("--on-disk"))
        {
            inMemory = false;
        }
        else
        {
            inMemory = true;
        }
        System.out.println("$> ");

        while ((statement = parser.Statement()) != null)
        {
            if (statement instanceof Select)
            {
                SelectProcessor  selectProcessor = new SelectProcessor(((Select) statement).getSelectBody(),inMemory,deleteExpression,updateQueries);
                RowTraverser RowIterator = selectProcessor.processQuery();
                Optimizer optimizer = new Optimizer(RowIterator, inMemory);
                RowIterator = optimizer.optimize();
                printResult(RowIterator);

            } else if (statement instanceof CreateTable)
            {
                String TableName = ((CreateTable) statement).getTable().getWholeTableName();
                TableInformation.addTableInfo(TableName, (CreateTable) statement);
            }
            else if (statement instanceof Insert)
            {
                String TableName = ((Insert) statement).getTable().getWholeTableName();
                HashMap<String,Integer> fieldPositionMapping = Utility.getFieldPostionMappingForJoin(TableName);
                String csvFile = TableName + "_insert" + ".dat";
                List<Column> columnName = ((Insert) statement).getColumns();
                csvWriter = new FileWriter(csvFile,true);
                BufferedWriter bw = new BufferedWriter(csvWriter);
                PrintWriter out = new PrintWriter(bw);
                String values = ((Insert) statement).getItemsList().toString().replace('(',' ');
                values = values.replace(')',' ');
                values = values.replace(" ","");
                String[] valuesToFile = values.split(",");
                String finalValue = "";
                for(Column column: columnName)
                {

                    if(fieldPositionMapping.containsKey(column.toString()))
                    {
                        int val = fieldPositionMapping.get(column.toString());
                        if(finalValue.equals("")){
                            finalValue = valuesToFile[val];
                        }
                        else
                        {
                            finalValue = finalValue + "|" +  valuesToFile[val];
                        }

                    }
                    else
                    {
                        finalValue = finalValue + "|" + null;
                    }
                }
                out.write(finalValue + '\n');
                out.close();

            }
            else if(statement instanceof Delete)
            {
                String TableName = ((Delete) statement).getTable().getWholeTableName();
                Expression whereExpression = ((Delete) statement).getWhere();
                InverseExpression inverseExpression = new InverseExpression(whereExpression);
                deleteExpressions.add(inverseExpression);
                deleteExpression.put(TableName,deleteExpressions);

            }
            else if(statement instanceof Update)
            {
                String TableName = ((Update) statement).getTable().getWholeTableName();
                String columns = ((Update) statement).getColumns().toString();

                List<Expression> expression = ((Update) statement).getExpressions();
                Expression whereCondition = ((Update) statement).getWhere();

                List<Field> tableColumns = TableInformation.getTableFieldMappingInfo(TableName);
                String updateQuery = "SELECT ";
                for(int i = 0; i< tableColumns.size(); i++)
                {
                   if(columns.contains(tableColumns.get(i).getColumnName()))
                   {
                       if(i+1 < tableColumns.size()) {
                           updateQuery = updateQuery + "CASE WHEN " + whereCondition.toString() + " THEN " + expression.remove(0) + " ELSE " + tableColumns.get(i).columnName + " END AS " + tableColumns.get(i).columnName + ",";
                       }
                       else
                       {
                           updateQuery = updateQuery + "CASE WHEN " + whereCondition.toString() + " THEN " + expression.remove(0) + " ELSE " + tableColumns.get(i).columnName + " END AS " + tableColumns.get(i).columnName;
                       }
                   }
                   else
                   {
                       if(i+1 < tableColumns.size())
                       {
                           updateQuery = updateQuery + tableColumns.get(i).columnName + " AS " + tableColumns.get(i).columnName+ " ,";
                       }
                       else
                       {
                           updateQuery = updateQuery + tableColumns.get(i).columnName + " AS " + tableColumns.get(i).columnName;
                       }
                   }
                }
                updateQuery = updateQuery + " FROM " + TableName;
                updateQueries.add(updateQuery);




            }

            System.out.println("$> ");


        }
    }

    public static void printResult(RowTraverser rowIterator)throws SQLException,IOException,ClassNotFoundException
    {
        while (rowIterator != null)
        {
            PrimitiveValue[]  dataRow = rowIterator.next();

            if(dataRow != null && dataRow.length!=0)
            {
                System.out.print(Utility.getLine(dataRow));
            }
            else
            {
                return;
            }
        }
        rowIterator.close();
    }


}