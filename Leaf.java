package de.tuda.dmdb.access.exercise;


import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.LeafBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 * @author cbinnig
 * Note: Leaf-level pointers omitted since AbstractUniqueIndex only supports single-key lookup
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T>{

	/**
	 * Leaf constructor
	 * @param uniqueBPlusTree .
	 */
	public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}

	@Override
	public AbstractRecord lookup(T key) {
		//TODO: implement this method
		AbstractPage Leaf_Page = this.getIndexPage();
		
		if(Leaf_Page.getNumRecords() != 0)
		{
			if (this.binarySearch(key) <= Leaf_Page.getNumRecords()-1) //Found condition
			{	
				//System.out.println("IM in");
				int Leaf_Record_SlotNumber = this.binarySearch(key);
				AbstractRecord temp_leaf_record = this.uniqueBPlusTree.getLeafRecPrototype().clone();
				Leaf_Page.read(Leaf_Record_SlotNumber, temp_leaf_record);
				T temp_cmp = (T)temp_leaf_record.getValue(0);
				if(key.compareTo(temp_cmp)!=0)
					return null;
				else
				{
					AbstractSQLValue Record_PageNr_IntObj = temp_leaf_record.getValue(1);
					AbstractSQLValue Record_SlotNr_IntObj = temp_leaf_record.getValue(2);
					SQLInteger Record_PageNr_Int = new SQLInteger();
					SQLInteger Record_SlotNr_Int = new SQLInteger();
					
					Record_PageNr_Int= (SQLInteger)Record_PageNr_IntObj; //PageNr of Record
					Record_SlotNr_Int= (SQLInteger)Record_SlotNr_IntObj; //SlotNr of Record
					
					AbstractPage Page_Data = BufferManager.getInstance().pin(Record_PageNr_Int.getValue());
					AbstractRecord Record_Found = this.uniqueBPlusTree.getTable().getPrototype();
					Page_Data.read(Record_SlotNr_Int.getValue(), Record_Found);
					return Record_Found;
				}
					
			}
			else 
				return null;
			
		}
		else
			return null;
		
	}
	
	@Override
	public boolean insert(T key, AbstractRecord record){
		
		//TODO: implement this method		
		AbstractPage Leaf_Page =  this.getIndexPage();  //The page is assigned to a leaf while constructing phase. //leaf page
		
		//check if the record is already in leaf_page
		if ( Leaf_Page.getNumRecords() != 0)
		{
//			if(Leaf_Page.getNumRecords()  >=  this.uniqueBPlusTree.getMaxFillGrade())
//			{
//				System.out.println("Leaf is already full, please split");
//				return false;
//			}
			AbstractRecord temp_leaf_record =  this.uniqueBPlusTree.getLeafRecPrototype().clone();
			//System.out.println(temp_leaf_record.getValue(0));
			//System.out.println(this.binarySearch(key));
			if (this.binarySearch(key) <= Leaf_Page.getNumRecords()-1) //Found condition
			{
				Leaf_Page.read(this.binarySearch(key), temp_leaf_record);
				T key_compare = (T) temp_leaf_record.getValue(0);
					if (key.compareTo(key_compare)==0)
					{
						return false;
						
					}
			}
		}
		 
		AbstractTable table = this.uniqueBPlusTree.getTable();					//insert data into table
		RecordIdentifier data_identify = table.insert(record);
		
		AbstractRecord Leaf_Record = this.uniqueBPlusTree.getLeafRecPrototype().clone(); // Pointer of the data.
		Leaf_Record.setValue(0, (T) key);
		//System.out.println(key);
		Leaf_Record.setValue(1,  new SQLInteger(data_identify.getPageNumber()));
		Leaf_Record.setValue(2, new SQLInteger(data_identify.getSlotNumber()));
		
		
		//System.out.println(this.binarySearch(key));
		Leaf_Page.insert(this.binarySearch(key),Leaf_Record,true);  //insert Pointer of the data to Leaf_page
		
		//Leaf_Page.read(this.binarySearch(key), temp_leaf_record);
		//System.out.println(key + "is" + temp_leaf_record.getValue(0));
		return true;
		
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}