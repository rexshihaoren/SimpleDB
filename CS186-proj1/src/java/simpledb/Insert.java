package simpledb;
import java.io.*;
import java.util.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean fetched = false;
    private TupleDesc td;
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.t=t;
        this.child=child;
        this.tableid=tableid;
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        String[] stringAr = new String[1];
        stringAr[0] = "number of inserted records";
        td = new TupleDesc(typeAr, stringAr);

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();

        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException{
        // some code goes here
        Tuple tup= new Tuple(td);
        try{
            int count = 0;
            if (fetched){
                return null;
            } else {
                fetched=true;
                while (child.hasNext()){
                    Tuple c=child.next();
                    try {    
                    Database.getBufferPool().insertTuple(t,tableid,c);
                    }
                    catch (IOException e){
                            e.printStackTrace();
                    }
                    count++;

                }
            	Field field = new IntField(count);
            	tup.setField(0, field);
            }
        }
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
