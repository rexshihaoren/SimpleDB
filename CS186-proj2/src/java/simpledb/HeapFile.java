package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    File f;
    TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        int id = f.getAbsoluteFile().hashCode();
        //System.out.println(id);
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
        try{

            RandomAccessFile rAf=new RandomAccessFile(f,"r");
            int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
            byte[] b=new byte[BufferPool.PAGE_SIZE];
            rAf.seek(offset);
            rAf.read(b, 0, BufferPool.PAGE_SIZE);
            HeapPageId hpid=(HeapPageId)pid;
            return new HeapPage(hpid, b);           
            }catch (IOException e){
                e.printStackTrace();
            }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        try {
            PageId pid= page.getId();
            HeapPageId hpid= (HeapPageId)pid;

            RandomAccessFile rAf=new RandomAccessFile(f,"rw");
            int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
            byte[] b=new byte[BufferPool.PAGE_SIZE];
            b=page.getPageData();
            rAf.seek(offset);
            rAf.write(b, 0, BufferPool.PAGE_SIZE);           
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length()/BufferPool.PAGE_SIZE);
   
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> affectedPages = new ArrayList<Page>();
        try{
            if (this.getEmptyPages(tid).isEmpty()){
                HeapPageId hpid=new HeapPageId(this.getId(), this.numPages());
                HeapPage hp=new HeapPage(hpid, HeapPage.createEmptyPageData());
                hp.insertTuple(t);
                this.writePage(hp);
                affectedPages.add(hp);  

            } else {
                Page p=this.getEmptyPages(tid).get(0);
                HeapPage hp=(HeapPage)p;
                hp.insertTuple(t);
                affectedPages.add(hp);       
            }
        }
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
        catch (IOException e){
            e.printStackTrace();
        }
            return affectedPages;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid=t.getRecordId();
        PageId pid=rid.getPageId();
        Page p=Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        HeapPage hp=(HeapPage)p;
        hp.deleteTuple(t);
        return Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    //I added this
    public ArrayList<Page> getEmptyPages(TransactionId tid) throws DbException,TransactionAbortedException{
        ArrayList<Page> pagelist=new ArrayList<Page>();
        try{
            int tableid=this.getId();
            for (int i=0; i<this.numPages();i++){
                HeapPageId pid= new HeapPageId(tableid,i);
                Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                if (((HeapPage)page).getNumEmptySlots()!=0){
                    pagelist.add(page);
                }
            }
        } 
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
            return pagelist;
    }
}