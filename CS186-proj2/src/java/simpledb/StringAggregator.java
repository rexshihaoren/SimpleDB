package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	private int gbField;
	private Type gbFieldType;
	private int aggregateField;
	private Op op;
	private boolean noGrouping = false;//true if NO_GROUPING
	private HashMap<Field, Integer> groups; //(groupValue, aggregateValue)
	private String fieldName="", groupFieldName="";
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
	if (what != Op.COUNT) {
		throw new IllegalArgumentException("op must be COUNT");
	}
	if (gbfield==Aggregator.NO_GROUPING) {
		noGrouping=true;
	}
	gbField=gbfield;
	gbFieldType=gbfieldtype;
	aggregateField = afield;
	op = what;
	groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	Field key;//tup's goupby field
	int currentAggregateValue;//current agg value for the key
	fieldName=tup.getTupleDesc().getFieldName(aggregateField);
	//find Field Key
	if (noGrouping) {
		key = new IntField(Aggregator.NO_GROUPING);
	} else {
		key = tup.getField(gbField);
		groupFieldName=tup.getTupleDesc().getFieldName(gbField);

	}	
	

	// find the current currentAggregateValue:
	if (groups.containsKey(key)) {
		currentAggregateValue = groups.get(key);
	} else {//groups does not yet contain this key
			groups.put(key, 0);
	}
	currentAggregateValue = groups.get(key);

		currentAggregateValue++;
		groups.put(key, currentAggregateValue);
    }

public TupleDesc getTupleDesc() {
	Type[] typeAr;
	String[] stringAr;
	TupleDesc td;

	if (noGrouping) {
		typeAr = new Type[1];
		stringAr = new String[1];
		typeAr[0] = Type.INT_TYPE;
		stringAr[0] = fieldName;//don't actually need real field name
	} else {
		typeAr = new Type[2];
		stringAr = new String[2];
		typeAr[0] = gbFieldType;
		typeAr[1] = Type.INT_TYPE;
		stringAr[0] = groupFieldName;
		stringAr[1] = fieldName;
	}
	td = new TupleDesc(typeAr, stringAr);
	return td;
}

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
	//from Aggregator.java, use TupleIterator.java
	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	TupleDesc td = this.getTupleDesc();

	if (noGrouping) {
		for (Field key : groups.keySet()){
			int value = groups.get(key);

			Tuple tuple = new Tuple(td);
			tuple.setField(0, new IntField(value));
			tuples.add(tuple);
		}
	} else {
		for (Field key : groups.keySet()){
			int value = groups.get(key);

			
			Tuple tuple = new Tuple(td);
			tuple.setField(0, key);
			tuple.setField(1, new IntField(value));
			tuples.add(tuple);


		}
	}
	return new TupleIterator(td, tuples);
    }

}
