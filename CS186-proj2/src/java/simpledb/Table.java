package simpledb;
import java.io.*;
import java.util.*;
//hahaha I added this class
/**
 * The Table info about tables 
 * Including DbFile file, String name, String pkeyField
 */

public class Table {

	private DbFile file;
	private String name;
	private String pkeyField;

	public Table(DbFile file, String name, String pkeyField){
		this.file=file;
		this.name=name;
		this.pkeyField=pkeyField;
	}

	public DbFile get_file(){
		return this.file;
	}

	public String get_name(){
		return this.name;
	}
	public String get_pkey(){
		return this.pkeyField;
	}





}