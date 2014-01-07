package simpledb;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.util.*;
import java.io.*;

public class NewCatalogTest extends SimpleDbTestBase {
    private static String name = "test";
	private String nameThisTestRun;
    
    @Before public void addTables() throws Exception {
        Database.getCatalog().clear();
		nameThisTestRun = SystemTestUtil.getUUID();
        Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new SkeletonFile(-2, Utility.getTupleDesc(2)), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(-2, Database.getCatalog().getTableId(name));
        assertEquals(-1, Database.getCatalog().getTableId(nameThisTestRun));
        
        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
        
        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /**
     * Unit test for Catalog.getDbFile()
     */
    @Test public void getDbFile() throws Exception {
        DbFile f = Database.getCatalog().getDbFile(-1);

        // NOTE(ghuo): we try not to dig too deeply into the DbFile API here; we
        // rely on HeapFileTest for that. perform some basic checks.
        assertEquals(-1, f.getId());
    }

    @Test public void new_getTupleDescTest() throws Exception {
        try {
            Database.getCatalog().getTupleDesc(100);
            Assert.fail("Should not find TupleDesc with id " + 100);
        } catch (NoSuchElementException e) {}
    }

    @Test public void new_getDbFileTest() throws Exception {
        try {
            Database.getCatalog().getDbFile(100);
            Assert.fail("Should not find DbFile with id " + 100);
        } catch (NoSuchElementException e) {}
    }

    @Test public void new_getPrimaryKeyTest() {
        String k = "key";
        Database.getCatalog().addTable(new SkeletonFile(-3, Utility.getTupleDesc(2)), "table1", k);

        String result = Database.getCatalog().getPrimaryKey(-3);
        assertEquals(k, result);
    }

    @Test public void new_getTableNameTest() {
        String name = "name1";
        Database.getCatalog().addTable(new SkeletonFile(-4, Utility.getTupleDesc(2)), name);

        String result = Database.getCatalog().getTableName(-4);

        assertEquals(name, result);
    }

    @Test public void new_clearTest() {
        Database.getCatalog().clear();
        try {
            Database.getCatalog().getDbFile(-1);
            Assert.fail("Should file since catalog is cleared.");
        } catch (NoSuchElementException e){}

        try {
            Database.getCatalog().getTupleDesc(-2);
            Assert.fail("Should file since catalog is cleared.");
        } catch (NoSuchElementException e){}
    }

    @Test public void new_tableIteratorTest() {
        Iterator<Integer> i = Database.getCatalog().tableIdIterator();

        int count = 0;
        while (i.hasNext()) {
            count++;
            i.next();
        }

        assertEquals(2, count);
    }


    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(NewCatalogTest.class);
    }
}
