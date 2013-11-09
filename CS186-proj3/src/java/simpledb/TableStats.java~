package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {
/** all the STATIC/FINAL variables/methods follows: **/
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

/**end all the STATIC/FINAL methods/variables **/
/**instance variables follows: **/

	private int tableId;
	private int ioCostPerPage;
	private HeapFile file;
	private TupleDesc tupleDesc;
	private int ntups=0;
	//String is the field name (from TupleDesc's fieldAr)
	private HashMap<String, Integer> minValues;
	private HashMap<String, Integer> maxValues;
	private HashMap<String, Object> histograms;
	

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        
        tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        file = (HeapFile)Database.getCatalog().getDbFile(tableid);
        tupleDesc = file.getTupleDesc();
        minValues = new HashMap<String, Integer>();
        maxValues = new HashMap<String, Integer>();
        histograms = new HashMap<String, Object>();
        
        Transaction transaction = new Transaction();
        TransactionId tid = transaction.getId();
        DbFileIterator it = file.iterator(tid);

	try {
        
        it.open(); //catch exceptions
        while (it.hasNext()){ //catch exceptions
        	Tuple tuple = it.next(); //catch exceptions
        	for (int i=0; i<tupleDesc.numFields(); i++) {
        		String fieldName = tupleDesc.getFieldName(i);
        		Type fieldType = tupleDesc.getFieldType(i);
        		
        		if (fieldType.equals(Type.INT_TYPE)) {
				int value = ((IntField)tuple.getField(i)).getValue();
        			if (!minValues.containsKey(fieldName) || value < minValues.get(fieldName)) {
        				minValues.put(fieldName, value);
        			}
        			if (!maxValues.containsKey(fieldName) || value > maxValues.get(fieldName)) {
        				maxValues.put(fieldName, value);
        			}
        		}
        	}
        }//closes while

        it.rewind(); //catch exceptions
        
	//create all the IntHistograms
	for (String key : minValues.keySet()) {
		int value = minValues.get(key);
		IntHistogram newIntHistogram = new IntHistogram(NUM_HIST_BINS, minValues.get(key), maxValues.get(key));
		histograms.put(key, newIntHistogram);
	}


	while (it.hasNext()){
		Tuple tuple = it.next();
		ntups++;
		for (int i=0; i<tupleDesc.numFields(); i++) {
			String fieldName = tupleDesc.getFieldName(i);
        		Type fieldType = tupleDesc.getFieldType(i);
			if (fieldType.equals(Type.INT_TYPE)) { //If INT_TYPE
				int value = ((IntField)tuple.getField(i)).getValue();
				IntHistogram intHistogram = (IntHistogram)histograms.get(fieldName);
				intHistogram.addValue(value);
				histograms.put(fieldName, intHistogram);
			} else { //STRING_TYPE
				String value = ((StringField)tuple.getField(i)).getValue();
				if (histograms.containsKey(fieldName)) {//histograms contains StringHistogram
					StringHistogram stringHistogram = (StringHistogram)histograms.get(fieldName);
					stringHistogram.addValue(value);
					histograms.put(fieldName, stringHistogram);
				} else {
					StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
					stringHistogram.addValue(value);
					histograms.put(fieldName, stringHistogram);
				}
			}
		}//closes for numFields()
	}//closes while loop

	} catch (DbException e) {
		e.printStackTrace();
	} catch (TransactionAbortedException e) {
		e.printStackTrace();
	}
        
 
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return file.numPages()*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        int ntups = totalTuples();
	return (int)Math.ceil(ntups*selectivityFactor);

    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
	//to implement???
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
	//field op constant
        // some code goes here
	String fieldName = tupleDesc.getFieldName(field);
	Type type = constant.getType();
	if (type==Type.INT_TYPE) {
		int value = ((IntField)constant).getValue();
		IntHistogram histogram = (IntHistogram)histograms.get(fieldName);
		return histogram.estimateSelectivity(op, value);
		
	} else {
		String value = ((StringField)constant).getValue();
		StringHistogram histogram = (StringHistogram)histograms.get(fieldName);
		return histogram.estimateSelectivity(op, value);
	}

    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
