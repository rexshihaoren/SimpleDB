package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * List of collection of fields 
     */
    private ArrayList<TDItem> fieldList;

    /**
     * A help class to facilitate organizing the information of each field
     * */

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return fieldList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        fieldList= new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem field = new TDItem(typeAr[i], fieldAr[i]);
            fieldList.add(field);
        }




    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        fieldList= new ArrayList<TDItem>();
        for (int i = 0; i< typeAr.length; i++){
            TDItem field = new TDItem(typeAr[i], "");
            fieldList.add(field);
        }
        
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0||i>fieldList.size()-1){
            throw new NoSuchElementException("Invalid field reference i");
        }
        return fieldList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0||i>fieldList.size()-1){
            throw new NoSuchElementException("Invalid field reference i");
        }
        return fieldList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(TDItem field : fieldList){
            int indexOfFiled = fieldList.indexOf(field);
            if (field.fieldName!=null){
                if(field.fieldName.equals(name)){
                    return indexOfFiled;
                }
            }else{
                if (name==null){
                    return indexOfFiled;
                }

            }  
        }

        throw new NoSuchElementException("no field with matching name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem field : fieldList){
            size+=field.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        int i;
        //Clever use of i and j.
        for(i = 0; i < td1.numFields(); i++){
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }
        
        for(int j = 0; j < td2.numFields(); j++){
            typeAr[i] = td2.getFieldType(j);
            fieldAr[i] = td2.getFieldName(j);
            i++;
        }
        
        //create a merged TupleDesc.
        return new TupleDesc(typeAr, fieldAr);


    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        //test if o is a TupleDesc obj
        if (o instanceof TupleDesc) {
            TupleDesc obj=(TupleDesc)o;
            //test size
            if ((obj.getSize()==this.getSize())){
                //test 1 to nth field type type
                for (int i=0;i<obj.numFields()-1;i++){
                    if (!this.getFieldType(i).equals(obj.getFieldType(i))){
                        //type mismatch
                        return false;
                    }
                }
                //same type, same size, ith field same type
                return true;
            } else {
                //size mismatch
                return false;
            }
        } else {
            //o is not TupleDesc
            return false;
        }   
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String descrip = "";
        for (int i =0; i< this.numFields();i++){
            descrip+=this.getFieldType(i).toString()+"("+this.getFieldName(i).toString()+")"+",";
        }

        
        descrip=descrip.substring(0, descrip.length() - 1);
        return descrip;
    }
}
