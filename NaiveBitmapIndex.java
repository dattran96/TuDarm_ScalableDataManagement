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
 * Bitmap index that uses the vanilla/naive bitmap approach (one bitmap for each distinct value)
 * @author melhindi
 *
 ** @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be used,
 * the implementation currently only support for SQLInteger type is guaranteed.
 */
public class NaiveBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T>{

	/*
	 * Constructor of NaiveBitmapIndex
	 * @param table Table for which the bitmap index will be build
	 * @param keyColumnNumber: index of the column within the passed table that should be indexed
	 */
	public NaiveBitmapIndex(AbstractTable table, int keyColumnNumber) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
		this.bitmapSize = this.getTable().getRecordCount();
		this.bulkLoadIndex();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		// TODO Auto-generated method stub
		ArrayList<Integer> Log_Loop = new ArrayList<Integer>();
		
		for(int i= 0; i<this.bitmapSize;i++)
		{
			if(Log_Loop.contains(i))
			{
				continue;
			}
			AbstractRecord Record_Table = this.table.getRecordFromRowId(i);
			T Key_HashTable = (T)Record_Table.getValue(keyColumnNumber);
			BitSet Value_HashTable = new BitSet(this.bitmapSize);
			Value_HashTable.set(i);
			for(int j=i+1; j< this.bitmapSize;j++)
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
					Value_HashTable_or.set(j);
					Value_HashTable.or(Value_HashTable_or);
					Log_Loop.add(j);
					System.out.println("Count j:" + j);
				}
			}
			this.bitMaps.put(Key_HashTable, Value_HashTable);
		}
}
	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		// TODO Auto-generated method stub
		 BitSet Value_HashTable = new BitSet(this.bitmapSize);
		 List ListOfRecord = new ArrayList<AbstractRecord>();
		 
		 Set<T> key_set = this.getBitMaps().keySet();
		 for(T key : key_set)
		 {
			 if(key.compareTo(startKey)>=0 && key.compareTo(endKey)<=0 )
			 {
				 Value_HashTable.or(this.getBitMaps().get(key));
			 }
		 }
		 //System.out.println("Bitmap:"+Value_HashTable.cardinality());
		 
		 for(int i= 0; i<this.bitmapSize;i++)
		 {
			 if(Value_HashTable.get(i))
			 {
				 ListOfRecord.add(this.table.getRecordFromRowId(i));
			 }
		 }
		return ListOfRecord;
	}
}
