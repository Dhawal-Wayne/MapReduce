import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class ratingsPairValue implements Writable {

	private long partialSum=0;
	private long partialCount=0;
	
	ratingsPairValue()
	{
		super();
	}
	
	ratingsPairValue(long pSum,long pCount)
	{
		this.partialSum = pSum;
		this.partialCount = pCount;
	}
	
	public long getPartialSum()
	{
		return this.partialSum;
	}
	
//	public void setPartialSum(long value)
//	{
//		this.partialSum = value;
//		return ;
//	}
//	
	public long getPartialCount()
	{
		return this.partialCount;
	}
//	
//	public void setPartialCount(long value)
//	{
//		this.partialCount = value;
//		return ;
//	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		this.partialSum = dataInput.readLong();
		this.partialCount = dataInput.readLong();

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		dataOutput.writeLong(this.partialSum);
		dataOutput.writeLong(this.partialCount);
	}

}
