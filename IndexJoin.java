package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.access.AbstractUniqueIndex;
import de.tuda.dmdb.sql.operator.IndexJoinBase;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

public class IndexJoin<T extends AbstractSQLValue> extends IndexJoinBase<T> {

	public IndexJoin(Operator child, int joinAttribute, AbstractUniqueIndex<T> index){
		super(child, joinAttribute, index);
	}

	@Override
	public void open() {
		//TODO
		this.getChild().open();
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractRecord next() {
		//TODO
		while(true)
		{
		AbstractRecord Rec_table = this.getChild().next();
		if(Rec_table==null)
			return null;
		AbstractRecord Rec_index= this.index.lookup((T) Rec_table.getValue(this.joinAttribute));
		if(Rec_index != null && Rec_index.getValue(this.joinAttribute).compareTo(Rec_table.getValue(this.joinAttribute))==0)
			return Rec_table.append(Rec_index);
		}
	}

	@Override
	public void close() {
		//TODO
	}
}
