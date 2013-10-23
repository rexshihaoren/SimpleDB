package simpledb;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

import java.io.*;
import java.util.*;

public class NewTupleTest extends SimpleDbTestBase {

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test public void modifyFields() {
        TupleDesc td = Utility.getTupleDesc(2);

        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));

        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));
    }

    /**
     * Unit test for Tuple.getTupleDesc()
     */
    @Test public void getTupleDesc() {
        TupleDesc td = Utility.getTupleDesc(5);
        Tuple tup = new Tuple(td);
        assertEquals(td, tup.getTupleDesc());
    }

    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0,0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

	try {
	    assertEquals(rid1, tup1.getRecordId());
	} catch (java.lang.UnsupportedOperationException e) {
		//rethrow the exception with an explanation
    	throw new UnsupportedOperationException("modifyRecordId() test failed due to " +
    			"RecordId.equals() not being implemented.  This is not required for Lab 1, " +
    			"but should pass when you do implement the RecordId class.");
	}
    }

    @Test public void new_toStringTest() {
        int length = 10;
        String name = "td";
        Tuple t = new Tuple(Utility.getTupleDesc(length, name));

        String tString = t.toString();

        // Last character should be \n.
        assertEquals("\n", tString.substring(tString.length()-1));
        
        // Only last character is \n.
        assertFalse(tString.substring(0, tString.length()-1).contains("\n"));

        // Split string on any white character.
        String[] tStringAr = tString.substring(0, tString.length()-1).split("\\s+");
        assertEquals(length, tStringAr.length);        
    }

    @Test public void new_fieldsTest() {
        int length = 10;
        String name = "td";
        Tuple t = new Tuple(Utility.getTupleDesc(length, name));

        Iterator<Field> fs = t.fields();

        int i = 0;
        while (fs.hasNext()) {
            i++;
            fs.next();
        }

        assertEquals(length, i);
    }

    /**
     * Helper class for iterator test.
     */
    private class TestField implements Field {
        public void serialize(DataOutputStream dos) throws IOException {}
        public boolean compare(Predicate.Op op, Field value) { return false; }
        public Type getType() { return null; }
        public int hashCode() { return 0; }
        public boolean equals(Object field) { return false; }
        public String toString() { return null; }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(NewTupleTest.class);
    }
}
