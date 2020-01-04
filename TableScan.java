package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.sql.operator.TableScanBase;
import de.tuda.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class TableScan extends TableScanBase {
	
	public TableScan(AbstractTable table){
		super(table);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(){
		//TODO
		tableIter = this.table.iterator();   //create a inner class
	}
	
	@Override
	public AbstractRecord next() {
		//TODO
		if(this.tableIter.hasNext())
		{
			return this.tableIter.next();
		}
		else
			return null;
		
		
	}
	@Override
	public void close() {
		//TODO
		tableIter= this.table.iterator();
	}
}
