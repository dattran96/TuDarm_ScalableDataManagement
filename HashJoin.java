package de.tuda.dmdb.sql.operator.exercise;

import java.util.HashMap;

import de.tuda.dmdb.sql.operator.HashJoinBase;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

public class HashJoin extends HashJoinBase {

	public HashJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	@Override
	public void open() {
		//TODO
		this.getLeftChild().open();
		this.getRightChild().open();
	}

	@Override
	public AbstractRecord next() {
		//TODO
		while(true)
		{
			AbstractRecord Right_Rec = this.getRightChild().next();
			if(Right_Rec==null)
			{
				break;
			}
			while(true)
			{
				AbstractRecord Left_Rec = this.getLeftChild().next();
				if(Left_Rec==null)
				{
					this.getLeftChild().open();
					break;
				}
				if(Left_Rec.getValue(leftAtt).compareTo(Right_Rec.getValue(rightAtt))==0)
				{
					AbstractRecord EquiJoin_Rec= Left_Rec.append(Right_Rec);
					this.getLeftChild().open();
					return EquiJoin_Rec;
				}
			}
		}
		return null;
	}

	@Override
	public void close() {
		//TODO
	}
}
