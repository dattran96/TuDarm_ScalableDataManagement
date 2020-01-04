package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.SelectionBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

@SuppressWarnings("unused")
public class Selection extends SelectionBase {
	public Selection(Operator child, int attribute, AbstractSQLValue constant) {
		super(child, attribute, constant);
	}

	@Override
	public void open() {
		//TODO
		 this.getChild().open();
	}

	@Override
	public AbstractRecord next() {
		//TODO
		while(true)
		{
			
			AbstractRecord Rec_Temp;
			Rec_Temp = this.getChild().next();
			if(Rec_Temp==null)
				break;
			if(Rec_Temp.getValue(this.attribute).compareTo(this.constant)==0  && Rec_Temp != null)
			{
				return Rec_Temp;
			}	
		}
		return null;
		
	}

	@Override
	public void close() {
		//TODO
	}
}
