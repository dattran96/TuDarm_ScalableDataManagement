package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Unique B+-Tree implementation 
 * @author cbinnig
 *
 * @param <T>
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {
	
	/**
	 * Constructor of B+-Tree with user-defined fil-grade
	 * @param table Table to be indexed
	 * @param keyColumnNumber Number of unique column which should be indexed
	 * @param fillGrade fill grade of index
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int fillGrade) {
		super(table, keyColumnNumber, fillGrade);
	} 
	
	/**
	 * Constructor for B+-tree with default fill grade
	 * @param table table to be indexed 
	 * @param keyColumnNumber Number of unique column which should be indexed
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
		this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
	}	
	
	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean insert(AbstractRecord record) {
		//TODO: implement this method
		//this.root.insert((T)record.getValue(this.KEY_POS), record);
		if(this.root.getIndexPage().getNumRecords() < this.maxFillGrade)
		{
			this.root.insert((T)record.getValue(this.keyColumnNumber), record);
		}
		
		else if(this.root.getIndexPage().getNumRecords() == this.maxFillGrade && this.indexElements.size() < this.maxFillGrade+1)
		{
			this.root.insert((T)record.getValue(this.keyColumnNumber), record);
			//create 2 leaves, 1 node, 1 temp_record , ready to copy and split
			AbstractIndexElement<T> NewNode = new Node <T>(this);
			AbstractIndexElement<T> indexElement1 = this.root.createInstance();
			AbstractIndexElement<T> indexElement2 = this.root.createInstance();
			this.root.split(indexElement1, indexElement2);  
			
			AbstractRecord tempRecord = this.getNodeRecPrototype().clone();
			tempRecord.setValue(this.KEY_POS,indexElement1.getMaxKey());
			tempRecord.setValue(this.PAGE_POS, new SQLInteger(indexElement1.getPageNumber()));
			NewNode.getIndexPage().insert(tempRecord);
			
			tempRecord.setValue(this.KEY_POS,indexElement2.getMaxKey());
			//System.out.println(indexElement2.getMaxKey());
			tempRecord.setValue(this.PAGE_POS, new SQLInteger(indexElement2.getPageNumber()));
			NewNode.getIndexPage().insert(tempRecord);
			
			NewNode.insert((T)(record.getValue(this.keyColumnNumber)), record);
			this.setRoot(NewNode);
		}
		else
			this.root.insert((T)record.getValue(this.KEY_POS), record);
		//System.out.println("Number of Record:"+ this.root.getIndexPage().getNumRecords());
		//System.out.println("Pointer:"+ Record_Temp.getValue(0));
		return true;
	}
	
	@Override
	public AbstractRecord lookup(T key) {
		//TODO: implement this method
		return this.root.lookup(key);
	}

}
