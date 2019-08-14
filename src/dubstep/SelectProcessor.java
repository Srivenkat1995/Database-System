package dubstep;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;


public class SelectProcessor
{
    public static final int FROM_TABLE = 201;
    public static final int FROM_JOIN = 202;
    public static final int FROM_SUBSELECT = 203;
    public static final int PLAIN_SELECT= 301;
    public static final int UNION_SELECT = 302;

    SelectBody selectBody;
    FromItem fromItem;
    String TableName;
    SelectBody subSelectBody;
    boolean isAllcolumns;
    List<SelectItem> selectItems;
    boolean hasWhereCondition;
    Expression whereCondition;
    FromItem rightItem;
    String rightTable;
    List<PlainSelect> PlainSelectList;
    boolean hasTableAlias;
    String LeftTableAlias;
    String RightTableAlias;
    Queue<String> tableNames = new LinkedList<>();
    Queue<String> conditionOnTables = new LinkedList<>();
    Queue<String> joinQueue = new LinkedList<>();
    Queue<Boolean> joinQueueAlias = new LinkedList<>();
    Queue<String>  joinQueueAliasName = new LinkedList<>();
    Queue<Integer> positionOnTables = new LinkedList<>();
    boolean isLeftCondition;
    boolean isRightCondition;
    boolean hasGroupBy;
    boolean hasHavingCondition;
    boolean hasLimitBy;
    List<Column> groupByColumns;
    boolean isAggregate;
    int selectType;
    int FromType;
    Boolean isJoinConditionPresent;
    Expression joinCondition;
    Boolean mixtureOfTables;
    boolean inMemory;
    boolean hasOrderBy;
    Limit limit;
    Long limitValue;
    Expression havingCondition;
    List<OrderByElement> orderByFields;
    HashMap<String,List<Expression>> deleteExpression;
    List<String> updateQueries;



    public SelectProcessor(SelectBody selectBody,boolean inMemory,HashMap<String,List<Expression>> deleteExpression, List<String> updateQueries)
    {
       this.selectBody = selectBody;
       this.inMemory = inMemory;
       this.deleteExpression = deleteExpression;
       this.updateQueries = updateQueries;

       parseSelectBody();
    }

    public String getTableName()
    {
       return TableName;
    }

    private void parseSelectBody()
    {
        if(selectBody instanceof PlainSelect)
        {
            selectType = PLAIN_SELECT;
            parseFromItem();
            parseSelectItem();
            parseWhereCondition();
            parseGroupBy();
            parseHavingCondition();
            parseOrderBy();
            parseLimitBy();

        }
        else if(selectBody instanceof Union)
        {
          selectType = UNION_SELECT;
          PlainSelectList = ((Union) selectBody).getPlainSelects();

        }
    }

    private void parseLimitBy()
    {

        limit = ((PlainSelect)selectBody).getLimit();
        if(limit != null)
        {
            limitValue = limit.getRowCount();
            hasLimitBy = true;
        }


    }

    private void parseHavingCondition() {

        havingCondition = ((PlainSelect) selectBody).getHaving();
        if(havingCondition != null)
        {
            hasHavingCondition = true;
        }
        else
        {
            hasHavingCondition  = false;
        }


    }

    private void parseOrderBy()
    {
        orderByFields = ((PlainSelect)selectBody).getOrderByElements();
        if(orderByFields !=null)
        {
            hasOrderBy = true;
        }

    }

    private void parseFromItem()
    {
        fromItem = ((PlainSelect)selectBody).getFromItem();
        List<Join> joins= ((PlainSelect) selectBody).getJoins();


        if(fromItem instanceof Table)
        {
            FromType = FROM_TABLE;
            TableName = ((Table)fromItem).getWholeTableName();
            tableNames.add(TableName);
            if(fromItem.getAlias() != null)
            {
                hasTableAlias = true;
                LeftTableAlias = fromItem.getAlias();
            }
        }
        else if(fromItem instanceof SubSelect)
        {
            FromType = FROM_SUBSELECT;
            subSelectBody = ((SubSelect) fromItem).getSelectBody();
        } 
        if (joins != null)
        {
            FromType = FROM_JOIN;
            for (Join join : joins)
            {
                rightItem = join.getRightItem();
                if (rightItem instanceof Table)
                {
                    rightTable = ((Table) rightItem).getWholeTableName();
                    joinQueue.add(rightTable);
                    tableNames.add(rightTable);
                    if(rightItem.getAlias() !=null)
                    {
                        RightTableAlias = rightItem.getAlias();
                        joinQueueAlias.add(true);
                        joinQueueAliasName.add(RightTableAlias);
                    }
                    else {
                        joinQueueAlias.add(false);
                    }

                }
            }

        }

    }

    private void parseGroupBy()
    {
        groupByColumns = ((PlainSelect)selectBody).getGroupByColumnReferences();
        if(groupByColumns != null)
        {
           hasGroupBy = true;
           isAllcolumns = true;
        }
    }

    private void parseSelectItem()
    {
        selectItems = ((PlainSelect)selectBody).getSelectItems();
        SelectItem selectItem = selectItems.iterator().next();

        if((selectItem instanceof AllColumns)|| (selectItem instanceof  AllTableColumns))
        {
            isAllcolumns = true;
        }
        else {

            isAllcolumns = false;
            Iterator<SelectItem> select = this.selectItems.iterator();

            while (select.hasNext())
            {
                selectItem = select.next();

                if((selectItem.toString().contains(TableName+".")))
                {
                    hasTableAlias = true;
                    LeftTableAlias = TableName;
                }

                Expression expression = ((SelectExpressionItem)selectItem).getExpression();
                if(expression instanceof Function)
                {
                    isAggregate = true;
                    isAllcolumns = true;
                }
            }
        }
    }
    private void parseWhereCondition()
    {

        whereCondition = ((PlainSelect) selectBody).getWhere();

        if(whereCondition != null)
        {
            hasWhereCondition = true;

        }
        else {

            hasWhereCondition = false;
        }

    }

    public RowTraverser processQuery() throws IOException, SQLException,ClassNotFoundException, ParseException
    {
        switch (selectType)
        {
            case PLAIN_SELECT: return executePlainSelectQuery();
            case UNION_SELECT: return  executeSelectUnionQuery();
        }

        return null;
    }



    private RowTraverser executePlainSelectQuery() throws IOException, SQLException,ClassNotFoundException, ParseException
    {
        RowTraverser rowIterator;
        switch (FromType)
        {
            case FROM_TABLE:
                List<RowTraverser> IteratorsList = new ArrayList<RowTraverser>();
                String csvFile = TableName + "_insert" + ".dat";
                File file = new File(csvFile);
                RowIterator newRowIterator;
                rowIterator = new RowIterator(TableName, Utility.getFieldPostionMappingForJoin(TableName));
                if(file.exists())
                {
                    IteratorsList.add(rowIterator);
                    newRowIterator = new RowIterator(csvFile,Utility.getFieldPostionMappingForJoin(TableName),Utility.getListFieldType(TableInformation.getTableFieldMappingInfo(TableName)));
                    IteratorsList.add(newRowIterator);
                    rowIterator = new UnionIterator(IteratorsList);
                }
                if(! deleteExpression.isEmpty())
                {
                    List<Expression> expressions = deleteExpression.get(TableName);
                    Expression newDeleteExpression = expressions.remove(0);
                    for(Expression expression:expressions)
                    {
                        newDeleteExpression = new AndExpression(newDeleteExpression,expression);
                    }
                    rowIterator = new FilterIterator(rowIterator,newDeleteExpression);
                }
                if(updateQueries != null && !updateQueries.isEmpty())
                {
                    for(String updateQuery: updateQueries)
                    {
                        StringReader stringReader = new StringReader(updateQuery);
                        CCJSqlParser parser1 = new CCJSqlParser(stringReader);
                        Statement updateStatement = parser1.Statement();
                        if(updateStatement instanceof Select)
                        {
                            SelectProcessor  selectProcessor = new SelectProcessor( ((Select) updateStatement).getSelectBody(),inMemory,deleteExpression,null);
                             rowIterator = selectProcessor.processQuery();
                        }

                    }
                }
                if (hasWhereCondition) {
                        rowIterator = new FilterIterator(rowIterator, whereCondition);
                    }

                    if (isAggregate || hasGroupBy) {
                        rowIterator = new AggregateIterator(rowIterator, selectItems, groupByColumns, inMemory);

                    }
                    if (hasHavingCondition) {
                        rowIterator = new FilterIterator(rowIterator, havingCondition);
                    }
                    if (hasOrderBy) {
                        rowIterator = new OrderByIterator(rowIterator, orderByFields, inMemory);

                    }
                    if (hasLimitBy) {
                        rowIterator = new LimitIterator(rowIterator, limitValue);
                    }
                    ProjectIterator projectIterator = new ProjectIterator(rowIterator, selectItems, isAllcolumns);
                    return projectIterator;




            case FROM_SUBSELECT:

                SelectProcessor subSelectProcessor = new SelectProcessor(subSelectBody,inMemory,null,null);
                RowTraverser DataRowIterator = subSelectProcessor.processQuery();

                if(hasWhereCondition)
                {
                    DataRowIterator = new FilterIterator(DataRowIterator,whereCondition);
                }
                if(isAggregate || hasGroupBy)
                {
                    DataRowIterator = new AggregateIterator(DataRowIterator,selectItems,groupByColumns,inMemory);
                }
                if(hasHavingCondition)
                {
                    DataRowIterator = new FilterIterator(DataRowIterator,havingCondition);
                }
                if(hasOrderBy)
                {
                    DataRowIterator = new OrderByIterator(DataRowIterator,orderByFields,inMemory);
                }
                if(hasLimitBy)
                {
                    DataRowIterator = new LimitIterator(DataRowIterator,limitValue);
                }
                projectIterator = new ProjectIterator(DataRowIterator,selectItems,isAllcolumns);
                return projectIterator;

            case FROM_JOIN:

                HashMap<String,Integer> fieldPositionMapping;
                if(hasTableAlias && !LeftTableAlias.equals(TableName))
                {
                    fieldPositionMapping = TableInformation.getFieldMappingwithAlias(TableName,LeftTableAlias);
                }
                else
                {
                    fieldPositionMapping = Utility.getFieldPostionMappingForJoin(TableName);
                }
                RowTraverser left = new RowIterator(TableName,fieldPositionMapping);
                String rightTableName = joinQueue.remove();
                Boolean isAliases = joinQueueAlias.remove();
                RowTraverser right = getRightTableIterator(rightTableName,isAliases.booleanValue());
                RowTraverser joinIterator = intialJoin(left,right);

                while(! joinQueue.isEmpty())
                {
                    String nextRightTableName = joinQueue.remove();
                    Boolean isNextAliases = joinQueueAlias.remove();
                    RowIterator newRight = getRightTableIterator(nextRightTableName,isAliases.booleanValue());
                    joinIterator = new JoinIterator(joinIterator,newRight);
                }

                if(hasWhereCondition)
                {
                    joinIterator = new FilterIterator(joinIterator,whereCondition);
                }

                if(isAggregate || hasGroupBy)
                {
                    joinIterator = new AggregateIterator(joinIterator,selectItems,groupByColumns,inMemory);

                }
                if(hasHavingCondition)
                {
                    joinIterator = new FilterIterator(joinIterator,havingCondition);
                }
                if(hasOrderBy)
                {
                    joinIterator = new OrderByIterator(joinIterator,orderByFields,inMemory);
                }
                if(hasLimitBy)
                {
                    joinIterator = new LimitIterator(joinIterator,limitValue);
                }

                projectIterator = new ProjectIterator(joinIterator,selectItems,isAllcolumns);
                return projectIterator;
                }

        return null;

    }

    private Boolean checkForOptimization() throws InstantiationException, IllegalAccessException
    {

        List<String> expressions = new ArrayList<>();
        HashMap<String,Integer> fieldPositionMapping = null;
        Expression leftExpression = null;
        Expression rightExpression = null;

        boolean isSingleValuePresent = true;
        if(whereCondition instanceof BinaryExpression)
        {

            leftExpression = ((BinaryExpression) whereCondition).getLeftExpression();
            rightExpression = ((BinaryExpression) whereCondition).getRightExpression();
            expressions.add(leftExpression.toString());
            expressions.add(rightExpression.toString());
        }
        Expression tempExpression = leftExpression;
        for(Iterator tableIterator = tableNames.iterator(); tableIterator.hasNext();)
        {
            fieldPositionMapping = Utility.getFieldPostionMappingForJoin((String) tableIterator.next());
            if(tempExpression.equals(leftExpression)) isLeftCondition = fieldPositionMapping.containsKey(tempExpression.toString());
            if(tempExpression.equals(rightExpression)) isRightCondition = fieldPositionMapping.containsKey(tempExpression.toString());
            isSingleValuePresent = isSingleValuePresent && fieldPositionMapping.containsKey(tempExpression.toString());
            tempExpression = rightExpression;
        }

        if(isLeftCondition && isRightCondition)
        {
            HashMap<String,Integer> newFieldPositionMapping;

            Queue<String> tempTableNames = tableNames.getClass().newInstance();
            for(String e : tableNames)
            {
                tempTableNames.add(e);
            }
            while (! tempTableNames.isEmpty())
            {
                String tableName =  tempTableNames.remove();
                fieldPositionMapping = Utility.getFieldPostionMappingForJoin(tableName);
                for (String s: expressions)
                {
                    if (fieldPositionMapping.containsKey(s)) {
                        conditionOnTables.add(tableName);
                        positionOnTables.add(fieldPositionMapping.get(s));
                    }
                }
            }
        }


        if(! isSingleValuePresent)
        {
            return true;
        }
        else
        {
            return false;
        }




    }

    private Boolean evaluate(Boolean mixture, String nextRightTableName) {

        this.mixtureOfTables =  mixture || conditionOnTables.contains(nextRightTableName);
        return mixture && conditionOnTables.contains(nextRightTableName);

    }

    private Boolean evaluateJoinCondition(String leftTable, String rightTable) throws FileNotFoundException, IOException
    {

        String[] value = this.joinCondition.toString().split(" = ");
        HashMap<String,Integer> fieldPositionMapping;

        Queue<String> tempTableNames = tableNames;
        for(String e : tableNames)
        {
            tempTableNames.add(e);
        }
        while (! tempTableNames.isEmpty())
        {
            String tableName =  tempTableNames.remove();
            fieldPositionMapping = Utility.getFieldPostionMappingForJoin(tableName);
            for (String s: value)
            {
                if (fieldPositionMapping.containsKey(s)) {
                    conditionOnTables.add(tableName);
                    positionOnTables.add(fieldPositionMapping.get(s));
                }
            }
        }

        mixtureOfTables = conditionOnTables.contains(leftTable) || conditionOnTables.contains(rightTable);
        return conditionOnTables.contains(leftTable) && conditionOnTables.contains(rightTable);




    }

    private JoinIterator intialJoin(RowTraverser left, RowTraverser right) throws  IOException,SQLException,ClassNotFoundException
    {
        JoinIterator joinIterator;
        joinIterator = new JoinIterator(left,right);
        return  joinIterator;

    }

    private RowIterator getRightTableIterator(String rightTableName, boolean isAliases) throws FileNotFoundException
    {
        HashMap<String, Integer> rightMapping;
        if(isAliases && !joinQueueAliasName.isEmpty())
        {
            rightMapping = TableInformation.getFieldMappingwithAlias(rightTableName, joinQueueAliasName.remove());
        }
        else {
            rightMapping = Utility.getFieldPostionMappingForJoin(rightTableName);
        }
        return new RowIterator(rightTableName,rightMapping);
    }

    private RowTraverser executeSelectUnionQuery()throws IOException, SQLException, ClassNotFoundException,ParseException
    {
        List<RowTraverser> IteratorsList = new ArrayList<RowTraverser>();

        for(PlainSelect select: PlainSelectList)
        {
            SelectProcessor selectProcessor = new SelectProcessor((SelectBody)select,inMemory,null,null);
            RowTraverser rowIterator = selectProcessor.processQuery();
            IteratorsList.add(rowIterator);
        }

        UnionIterator unionIterator = new UnionIterator(IteratorsList);

        return unionIterator;
    }
}
