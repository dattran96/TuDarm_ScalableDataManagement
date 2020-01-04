package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.util.*;

/**
 * Bitmap index that uses the range encoded approach (still one bitmap for each distinct value)
 * @author lthostrup
 *
 ** @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be used,
 * the implementation currently only support for SQLInteger type is guaranteed.
 */
public class RangeEncodedBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T>{

	/*
	 * Constructor of NaiveBitmapIndex
	 * @param table Table for which the bitmap index will be build
	 * @param keyColumnNumber: index of the column within the passed table that should be indexed
	 */
public RangeEncodedBitmapIndex(AbstractTable table, int keyColumnNumber) {
	super(table, keyColumnNumber);
	this.bitMaps = new TreeMap<T, BitSet>(); //Use TreeMap to get an ordered map impl.
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
		
		for(int k=0;k < this.bitmapSize;k++)
		{
			AbstractRecord Record_temp1= this.table.getRecordFromRowId(k);
			T Key_HashTable_cmp1=(T)Record_temp1.getValue(keyColumnNumber);
			if(Key_HashTable_cmp1.compareTo(Key_HashTable)>=0)
			{
				Value_HashTable.set(k);
			}
		}
		
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
				Log_Loop.add(j);
			}
		}
		this.bitMaps.put(Key_HashTable, Value_HashTable);
	}
	for(T key: this.getBitMaps().keySet())
	{
		System.out.print("ListOfKeyInHashMap:"+key);
	}
}

@Override
public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
	// TODO Auto-generated method stub
	 BitSet Value_HashTable = new BitSet(this.bitmapSize);
	 List ListOfRecord = new ArrayList<AbstractRecord>();
	 List Min_Max = new ArrayList<Integer>();
	 Set<T> key_set = this.getBitMaps().keySet();
	 
	 
	 for(T key : key_set)
	 {
		 if(key.compareTo(startKey)>=0)
		 {
			 Value_HashTable.or(this.getBitMaps().get(key));
			 break;
		 }
	 }
	 for(T key : key_set)
	 {
		 if(key.compareTo(endKey)> 0)
		 {
			 Value_HashTable.andNot(this.getBitMaps().get(key));
			 break;
		 }
	 }
	 
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
