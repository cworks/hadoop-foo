/**	
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Class: AtBatMapper
 * File: AtBatMapper.java
 * Created: 2013-03-05 20:17:58.448540
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Description: format of Batting.csv file, first line is header and the rest is data
 * playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old
 * aaronha01,1959,1,ML1,NL,154,154,629,116,223,46,7,39,123,8,0,51,54,17,4,0,9,19,154
 * @author corbett
 */
public class AtBatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	/*
	 * number of columns in batting file
	 */
	private static final int COLUMN_COUNT = 24;
	/*
	 * playerID position in csv row
	 */
	private static final int PLAYER_ID = 0;
	/*
	 * AB position in csv row
	 */
	private static final int AB = 7;

	/*
	* The map method runs once for each line of text in the input file.
	* The method receives a key of type LongWritable, a value of type
	* Text, and a Context object.
	*/
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		/*
		 * skip header
		 */
		long lineNum = key.get();
		if(lineNum <= 1)
			return;

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();

		/*
		 * The line.split(",") splits the line on commas
		 */
		String batting[] = line.split(",");
		if(batting.length != COLUMN_COUNT) {
			System.out.println("Line: " + lineNum
				+ " contains bad data! Should have "
				+ COLUMN_COUNT + " fields but only found "
				+ batting.length);
			return;
		}
		String playerId = batting[PLAYER_ID];
		String atBat = batting[AB];
		if(playerId != null && atBat != null) {
		    /*
		     * Call the write method on the Context object to emit a key
		     * and a value from the map method.
		     */
		    if("".equals(atBat)) {
		    	atBat = "0";
		    }
			context.write(new Text(playerId), new IntWritable(Integer.parseInt(atBat)));		
		}

	}
}
