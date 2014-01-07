
package simpledb;

import java.util.*;
import java.io.*;


public class HeapPageIterator<Tuple> implements Iterator<Tuple>{
    ArrayList<Tuple> tuples;
    Iterator<Tuple> i;
    public HeapPageIterator(ArrayList<Tuple> tuples) {
        this.tuples = tuples;
        i = tuples.iterator();
    }

    @Override
    public boolean hasNext() {
        return i.hasNext();
    }

    @Override
    public Tuple next() {
        return i.next();
    }

    @Override
    public void remove() {
    throw new UnsupportedOperationException("remove");
    }
}