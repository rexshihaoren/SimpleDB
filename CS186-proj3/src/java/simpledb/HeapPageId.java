package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    //Need to initialize param needed: tableId, pgNo, and hashcode.
    //I added this
    private int tableId;
    private int pgNo;
    private int hashcode;
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId=tableId;
        this.pgNo=pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        //System.out.println(tableId);
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        Long longcode=Long.parseLong(String.valueOf(tableId)+ String.valueOf(pgNo));
        int intcode=longcode.hashCode();
        //System.out.println(intcode);
        return intcode;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here        
        //check if it's a HeapPageId
        if (o instanceof HeapPageId){
            HeapPageId hpIdObj=(HeapPageId)o;
            if(hpIdObj.getTableId()==this.getTableId() && hpIdObj.pageNumber()==this.pageNumber()){
                return true;
            }else{
                //tableId or pageNumber are different
                return false;
            }
        } else{
            //object o is not a HeapPageId
            return false;
        }
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
