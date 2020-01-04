package de.tuda.dmdb.access.exercise;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Bitmap that uses the approximate bitmap index (compressed) approach
 * @author melhindi
 *
 * @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be used,
 * the implementation currently only support for SQLInteger type is guaranteed.
 */
public class ApproximateBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

	/*
	 * Constructor of ApproximateBitmapIndex
	 * This implementation uses modulo as hash function and only supports SQLInteger as data type
	 * @param table Table for which the bitmap index will be build
	 * @param keyColumnNumbner: index of the column within the passed table that should be indexed
	 * @param bitmapSize Size of for each bitmap, i.e., use (% bitmapSize) as hashfunction
	 */
	public ApproximateBitmapIndex(AbstractTable table, int keyColumnNumber, int bitmapSize) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
		this.bitmapSize = bitmapSize;
		this.bulkLoadIndex();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		// TODO
		ArrayList<Integer> Log_Loop = new ArrayList<Integer>();
		int TableSize = this.table.getRecordCount();
		
		for(int i= 0; i<TableSize;i++)
		{
			if(Log_Loop.contains(i))
			{
				continue;
			}
			AbstractRecord Record_Table = this.table.getRecordFromRowId(i);
			T Key_HashTable = (T)Record_Table.getValue(keyColumnNumber);
			BitSet Value_HashTable = new BitSet(this.bitmapSize);
			Value_HashTable.set(i%this.bitmapSize);
			for(int j=i+1; j< TableSize;j++)
			{
				if(Log_Loop.contains(j))
				{	
					continue;
				}
				AbstractRecord Record_Table_cmp = this.table.getRecordFromRowId(j);
				T Key_HashTable_cmp = (T)Record_Table_cmp.getValue(keyColumnNumber);
				if(Key_HashTable_cmp.compareTo(Key_HashTable)==0)
				{
					BitSet Value_HashTable_or = new BitSet(this.bitmapSize);
					Value_HashTable_or.set(j%this.bitmapSize);
					Value_HashTable.or(Value_HashTable_or);
					Log_Loop.add(j);
					System.out.println("Count j:" + j);
				}
			}
			this.bitMaps.put(Key_HashTable, Value_HashTable);
		}
		
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) 
	{
	// TODO
	 BitSet Value_HashTable = new BitSet(this.bitmapSize);
	 BitSet Vaue_HashTable_Recover = new BitSet(this.getTable().getRecordCount());
	 
	 List ListOfRecord = new ArrayList<AbstractRecord>();
	 
	 //Combine Condition, using OR-OPERATOR to get record
	 Set<T> key_set = this.getBitMaps().keySet();
	 for(T key : key_set)
	 {
		 if(key.compareTo(startKey)>=0 && key.compareTo(endKey)<=0 )
		 {
			 Value_HashTable.or(this.getBitMaps().get(key));
		 }
	 }
	 
	 //POST-PREDICATE, check record in table again to avoid false positive
	 for(int l=0;l<this.bitmapSize;l++)
	 {
		 if(Value_HashTable.get(l))
		 {			 
			 for(int o=0;;o++)
			 {
				 if(l+o*this.bitmapSize >= this.getTable().getRecordCount())
				{
					 break;
				}
				AbstractRecord Record_Validate = this.table.getRecordFromRowId(l+o*this.bitmapSize);
				T Key_HashTable_cmp = (T)Record_Validate.getValue(keyColumnNumber);
				 if(Key_HashTable_cmp.compareTo(startKey)>=0 && Key_HashTable_cmp.compareTo(endKey)<=0)
				 {
					 Vaue_HashTable_Recover.set(l+o*this.bitmapSize);
				 }
						
			 }
			 
		 }
	 }
	 //Return records
	 for(int i= 0; i<this.getTable().getRecordCount();i++)
	 {
		 if(Vaue_HashTable_Recover.get(i))
		 {
			ListOfRecord.add(this.table.getRecordFromRowId(i)); 
		 }
	 }
	return ListOfRecord;
	}
	
}
