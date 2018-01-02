package q2;
import org.apache.commons.lang.math.NumberUtils;

public class TrainRecordParser {
  
  
  private String trainDirection;
  private int trainSpeed;
  private int trainTime;
  private int trainNumber;
  private String trainTimeHour;
  
  public void parse(String record) {
	  trainNumber = Integer.parseInt(record.substring(0, 4));
	  trainTime = Integer.parseInt(record.substring(7, 11));
	  trainSpeed= Integer.parseInt(record.substring(14, 16));
	  trainDirection=record.substring(19, 20);
	  trainTimeHour = record.substring(7, 9);


  }
  
 

  public boolean isValidRecord(String record) {
	  return (NumberUtils.isNumber(record.substring(0, 4)) 
			  && NumberUtils.isNumber(record.substring(7,11))
			  && NumberUtils.isNumber(record.substring(14,16))
			  && record.substring(19,20).matches("[NEWS]"));
	
	  }
  
  public String gettrainDirection() {
    return trainDirection;
  }

  public int gettrainSpeed() {
	    return trainSpeed;
	  }
  public int gettrainTime() {
	    return trainTime;
	  }
  public int gettrainNumber() {
	    return trainNumber;
	  }
  public String gettrainTimeHour() {
	    return trainTimeHour;
	  }
}
