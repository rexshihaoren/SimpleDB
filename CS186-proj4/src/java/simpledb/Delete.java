package simpledb;
import java.io.*;
import java.util.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */

public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;

    private boolean fetched = false;
    private TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t=t;
        this.child=child;
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        String[] stringAr = new String[1];
        stringAr[0] = "number of deleted records";
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
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

                        Database.getBufferPool().deleteTuple(t,c);
                        count++;
                    }
                    catch (TransactionAbortedException e){
                        e.printStackTrace();
                    }

                }
            	Field field = new IntField(count);
            	tup.setField(0, field);
            }
        }
        catch (DbException e){
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
        // some code goes here
        this.child = children[0];
    }

}
