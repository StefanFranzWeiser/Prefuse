package test.prefuse.data.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import prefuse.data.Table;
import prefuse.data.Tuple;
import prefuse.data.expression.parser.ExpressionParser;
import junit.framework.TestCase;

public class FilterIteratorFactoryTest extends TestCase {

    /** must be over 300 (= optimization threshold) */
    private static final int SIZE = 400;

    private Table table;

    protected void setUp() throws Exception {
        super.setUp();
        table = new Table();
        table.addColumn("string", String.class);
        table.addColumn("int", int.class);
        table.addColumn("long", long.class);

        for (int i = 0; i < SIZE; i++) {
            int row = table.addRow();
            table.set(row, "string", Integer.toString((int) Math.floor(Math.random() * 10)));
            table.setInt(row, "int", (int) Math.floor(Math.random() * 10));
            table.setLong(row, "long", (long) Math.floor(Math.random() * 10));
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        table = null;
    }
    
    public void testObjectFilterNoIndex() {
        Iterator iter = table.tuples(ExpressionParser.predicate("[string]='3'"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).get("string"), "3");
        }
    }
    
    public void testObjectFilterIndex() {
        table.index("string");
        Iterator iter = table.tuples(ExpressionParser.predicate("[string]='4'"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).get("string"), "4");
        }
    }

    public void testIntFilterNoIndex() {
        Iterator iter = table.tuples(ExpressionParser.predicate("[int]=3"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).getInt("int"), 3);
        }
    }
    
    public void testIntFilterIndex() {
        table.index("int");
        Iterator iter = table.tuples(ExpressionParser.predicate("[int]=4"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).getInt("int"), 4);
        }
    }

    public void testLongFilterNoIndex() {
        Iterator iter = table.tuples(ExpressionParser.predicate("[long]=3"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).getLong("long"), 3);
        }
    }
    
    public void testLongFilterIndex() {
        table.index("long");
        Iterator iter = table.tuples(ExpressionParser.predicate("[long]=4"));
        while (iter.hasNext()) {
            assertEquals(((Tuple)iter.next()).getLong("long"), 4);
        }
    }
}
