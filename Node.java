package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.NodeBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index node
 * @author cbinnig
 *
 */
public class Node<T extends AbstractSQLValue> extends NodeBase<T>{

	/**
	 * Node constructor
	 * @param uniqueBPlusTree TODO
	 */
	public Node(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}
	
	
	@Override
	public AbstractRecord lookup(T key) {
		//TODO: implement this method
		int find_slot_to_branch = 0;
		find_slot_to_branch = this.binarySearch(key);
		AbstractRecord Node_Record = this.uniqueBPlusTree.getNodeRecPrototype().clone();
		this.getIndexPage().read(find_slot_to_branch,Node_Record);
		SQLInteger Leaf_Page_Number = (SQLInteger)Node_Record.getValue(1);
		AbstractIndexElement<T> Leaf_Obj= this.uniqueBPlusTree.getIndexElement(Leaf_Page_Number.getValue());
		return Leaf_Obj.lookup(key);
}
	
	@Override
	public boolean insert(T key, AbstractRecord record){
		//TODO: implement this method
		AbstractTable table = this.uniqueBPlusTree.getTable();					//insert data into table
		RecordIdentifier data_identify = table.insert(record);
		
		AbstractRecord Leaf_Record = this.uniqueBPlusTree.getLeafRecPrototype().clone(); // Pointer of the data.
		Leaf_Record.setValue(0, (T) key);
		Leaf_Record.setValue(1,  new SQLInteger(data_identify.getPageNumber()));
		Leaf_Record.setValue(2, new SQLInteger(data_identify.getSlotNumber()));

		//AbstractIndexElement<T>Leaf_Obj = null ;
		int Slot_Node=0;
		AbstractRecord Node_Record = this.uniqueBPlusTree.getNodeRecPrototype().clone();
		
		Slot_Node= this.binarySearch(key); 
		this.getIndexPage().read(Slot_Node,Node_Record);
		SQLInteger Leaf_Page_Number = (SQLInteger)Node_Record.getValue(1);
		AbstractIndexElement<T> Leaf_Obj= this.uniqueBPlusTree.getIndexElement(Leaf_Page_Number.getValue());
		Leaf_Obj.insert(key,record);
		
		if(Leaf_Obj.getIndexPage().getNumRecords() > this.uniqueBPlusTree.getMaxFillGrade())
		{
			//System.out.println("Split child");
			AbstractIndexElement<T> Leaf_part1 =  Leaf_Obj.createInstance();
			AbstractIndexElement<T> Leaf_part2 =  Leaf_Obj.createInstance();
			Leaf_Obj.split(Leaf_part1,Leaf_part2);
			AbstractRecord nodeRecord = this.uniqueBPlusTree.getNodeRecPrototype().clone();
			nodeRecord.setValue(this.uniqueBPlusTree.KEY_POS, Leaf_part1.getMaxKey());
			nodeRecord.setValue(this.uniqueBPlusTree.PAGE_POS, new SQLInteger(Leaf_part1.getPageNumber()));
			this.getIndexPage().insert(Slot_Node, nodeRecord, false);
			nodeRecord.setValue(this.uniqueBPlusTree.KEY_POS, Leaf_part2.getMaxKey());
			nodeRecord.setValue(this.uniqueBPlusTree.PAGE_POS, new SQLInteger(Leaf_part2.getPageNumber()));
			this.getIndexPage().insert(Slot_Node+1, nodeRecord, true);
		}
		
		if(this.getIndexPage().getNumRecords() > this.uniqueBPlusTree.getMaxFillGrade()+1)
		{
			System.out.println("Split Parents");
			AbstractIndexElement<T> Node_part1 = this.createInstance();
			AbstractIndexElement<T> Node_part2 = this.createInstance();
			//AbstractIndexElement<T> temp_Node = this.createInstance();
			
			Node<SQLInteger> temp_Node = new Node(this.uniqueBPlusTree);
			this.split(Node_part1, Node_part2);
			AbstractRecord nodeRecord = this.uniqueBPlusTree.getNodeRecPrototype().clone();
			nodeRecord.setValue(this.uniqueBPlusTree.KEY_POS, Node_part1.getMaxKey());
			nodeRecord.setValue(this.uniqueBPlusTree.PAGE_POS, new SQLInteger(Node_part1.getPageNumber()));
			//temp_Node.getIndexPage().insert(0, Node_Record, false);
			temp_Node.getIndexPage().insert(nodeRecord);

			nodeRecord.setValue(this.uniqueBPlusTree.KEY_POS, Node_part2.getMaxKey());
			nodeRecord.setValue(this.uniqueBPlusTree.PAGE_POS, new SQLInteger(Node_part2.getPageNumber()));
			//temp_Node.getIndexPage().insert(1, Node_Record, false);
			temp_Node.getIndexPage().insert(nodeRecord);

			this.indexPageNumber = 	temp_Node.getPageNumber();	
			
			//BufferManager.getInstance().unpin(temp_Node.getPageNumber());
			//this.uniqueBPlusTree.getIndexElements().remove(temp_Node.getPageNumber());
			//this.indexPageNumber =1 ;
		}
			return true;
		
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}
	
}
