package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;

public interface RowTraverser extends Serializable, Cloneable
{
    PrimitiveValue[] next()throws SQLException, IOException, ClassNotFoundException;
    void reset()throws IOException,SQLException,ClassNotFoundException;
    HashMap<String, Integer> getFieldPositionMapping();
    void close() throws IOException, ClassNotFoundException;
    PrimitiveValue[] getcurrent();

}
