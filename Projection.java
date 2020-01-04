package de.tuda.dmdb.sql.operator.exercise;

import java.util.Vector;

import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.ProjectionBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;

@SuppressWarnings("unused")
public class Projection extends ProjectionBase {
	
	public Projection(Operator child, Vector<Integer> attributes) {
		super(child, attributes);
	}
	
	@Override
	public void open(){
		//TODO
		this.getChild().open();
	}

	@Override
	public AbstractRecord next() {
		//TODO
		AbstractRecord Projection_Rec = new Record(this.attributes.size());
		AbstractRecord temp_Rec = this.getChild().next();
		if(temp_Rec==null)
			return null;
		else
		{
			int j=0;
			for(int i : this.attributes)
			{
				Projection_Rec.setValue(j, temp_Rec.getValue(i));
				j++;
			}
			return Projection_Rec;
		}		
	}

	@Override
	public void close() {
		//TODO
	}
}
