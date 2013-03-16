/**	
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Class: PlayerBattingStatsTest
 * File: PlayerBattingStatsTest.java
 * Created: 2013-03-08 16:51:19.721845
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Description: MRUnit test for PlayerBattingMapper and PlayerBattingReducer
 * @author corbett
 */ 
public class PlayerBattingStatsTest {
    /*
     * header from Batting.csv
     */
    private static final String BATTING_HEADER =
        "playerID,yearID,stint,teamID,lgID,G,G_batting,AB," +
        "R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old";
    /*
     * example data from Batting.csv
     */
    private static final String BATTING_DATA[] = new String[] { 
        "aardsda01,2004,1,SFN,NL,11,11,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11",
        "aardsda01,2006,1,CHN,NL,45,43,2,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,45",
        "aardsda01,2007,1,CHA,AL,25,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2",
        "aardsda01,2008,1,BOS,AL,47,5,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,5",
        "aardsda01,2009,1,SEA,AL,73,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,",
        "aardsda01,2010,1,SEA,AL,53,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,",
        "aaronha01,1954,1,ML1,NL,122,122,468,58,131,27,6,13,69,2,2,28,39,,3,6,4,13,122",
        "aaronha01,1955,1,ML1,NL,153,153,602,105,189,37,9,27,106,3,1,49,61,5,3,7,4,20,153",
        "aaronha01,1956,1,ML1,NL,153,153,609,106,200,34,14,26,92,2,4,37,54,6,2,5,7,21,153",
        "aaronha01,1957,1,ML1,NL,151,151,615,118,198,27,6,44,132,1,1,57,58,15,0,0,3,13,151",
        "aaronha01,1958,1,ML1,NL,153,153,601,109,196,34,4,30,95,4,1,59,49,16,1,0,3,21,153",
        "aaronha01,1959,1,ML1,NL,154,154,629,116,223,46,7,39,123,8,0,51,54,17,4,0,9,19,154",
        "aaronha01,1960,1,ML1,NL,153,153,590,102,172,20,11,40,126,16,7,60,63,13,2,0,12,8,153",
        "aaronha01,1961,1,ML1,NL,155,155,603,115,197,39,10,34,120,21,9,56,64,20,2,1,9,16,155",
        "aaronha01,1962,1,ML1,NL,156,156,592,127,191,28,6,45,128,15,7,66,73,14,3,0,6,14,156",
        "aaronha01,1963,1,ML1,NL,161,161,631,121,201,29,4,44,130,31,5,78,94,18,0,0,5,11,161",
        "aaronha01,1964,1,ML1,NL,145,145,570,103,187,30,2,24,95,22,4,62,46,9,0,0,2,22,145",
        "aaronha01,1965,1,ML1,NL,150,150,570,109,181,40,1,32,89,24,4,60,81,10,1,0,8,15,150",
        "aaronha01,1966,1,ATL,NL,158,158,603,117,168,23,1,44,127,21,3,76,96,15,1,0,8,14,158",
        "aaronha01,1967,1,ATL,NL,155,155,600,113,184,37,3,39,109,17,6,63,97,19,0,0,6,11,155",
        "aaronha01,1968,1,ATL,NL,160,160,606,84,174,33,4,29,86,28,5,64,62,23,1,0,5,21,160",
        "aaronha01,1969,1,ATL,NL,147,147,547,100,164,30,3,44,97,9,10,87,47,19,2,0,3,14,147",
        "aaronha01,1970,1,ATL,NL,150,150,516,103,154,26,1,38,118,9,0,74,63,15,2,0,6,13,150",
        "aaronha01,1971,1,ATL,NL,139,139,495,95,162,22,3,47,118,1,1,71,58,21,2,0,5,9,139",
        "aaronha01,1972,1,ATL,NL,129,129,449,75,119,10,0,34,77,4,0,92,55,15,1,0,2,17,129",
        "aaronha01,1973,1,ATL,NL,120,120,392,84,118,12,1,40,96,1,1,68,51,13,1,0,4,7,120",
        "aaronha01,1974,1,ATL,NL,112,112,340,47,91,16,0,20,69,1,0,39,29,6,0,1,2,6,112",
        "aaronha01,1975,1,ML4,AL,137,137,465,45,109,16,2,12,60,0,1,70,51,3,1,1,6,15,137",
        "aaronha01,1976,1,ML4,AL,85,85,271,22,62,8,0,10,35,0,1,35,38,1,0,0,2,8,85"
    };
    /*
     * example of valid but blank data in Batting.csv, blanks should render as 0
     */
    private static final String BLANK_BATTING_DATA = 
        "aasedo01,1977,1,BOS,AL,13,0,,,,,,,,,,,,,,,,,13";

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    private MapDriver<LongWritable, Text, Text, Text> mapFunction;
    private ReduceDriver<Text, Text, Text, Text> reduceFunction;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrFunction;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

        /*
         * Set up the mapper test harness
         * in: LongWritable: LineNumber
         * in: Text: See BATTING_DATA, comma delimited player batting data by year
         * emit: Text: playerId
         * emit: Text: BATTING_DATA we want to summarize in reducer
         */
        PlayerBattingMapper mapper = new PlayerBattingMapper();
        mapFunction = new MapDriver<LongWritable, Text, Text, Text>();
        mapFunction.setMapper(mapper);

        /*
         * Set up the reducer test harness.
         * in: Text: playerId
         * in: Iterable<Text>: Iterable of mapped batting stats per player
         * out: Text: playerId
         * out: Text: career batting stats
         */
        PlayerBattingReducer reducer = new PlayerBattingReducer();
        reduceFunction = new ReduceDriver<Text, Text, Text, Text>();
        reduceFunction.setReducer(reducer);

        /*
         * Set up the mapper/reducer test harness.
         */
        mrFunction = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
        mrFunction.setMapper(mapper);
        mrFunction.setReducer(reducer);
    }
 
    /**
     * Test mapper
     */
    @Test
    public void testMapper() {

        /*
         * For this test we build mapper input from BATTING_DATA
         */
        mapFunction.withInput(new LongWritable(29), new Text(BATTING_DATA[28]));
    
        /*
         * We expect this output for the given line
         */
        mapFunction.withOutput(new Text("aaronha01"),
            new Text("85,271,22,62,8,0,10,35,0,1,35,38,1,0,0,2,8"));
        /*
         * Run the test.
         */
        mapFunction.runTest();
    } 

    /**
     * Some lines in Batting.csv have 23 fields instead of 24, these lines are conidered fine
     * because the 24th field is redundant so it can be ignored.  However when the 24th element
     * is absent sometimes the line ends in a comma, for example: ...,0,0,0,
     * This test is to proove those line are handled correctly.
     */
    @Test
    public void testMapperOnLineEndingWithComma() {

        mapFunction.withInput(new LongWritable(5), new Text(BATTING_DATA[4]));
        mapFunction.withOutput(new Text("aardsda01"),
            new Text("73,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"));
        mapFunction.runTest();
    }

    /**
     * Some lines in Batting.csv have blank fields, for example they may have something like:
     * 4,,,,33 which is valid.  However the Mapper should read these lines and render blanks as
     * zeros, for example the previous snippet should render as 4,0,0,0,33
     * This test is to proove these lines are dealt with correctly.
     */
    @Test
    public void testMapperOnLineWithBlankBattingData() {
        mapFunction.withInput(new LongWritable(2), new Text(BLANK_BATTING_DATA));
        mapFunction.withOutput(new Text("aasedo01"),
            new Text("13,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"));
        mapFunction.runTest();      
    }

    /**
     * Test reducer
     */
    @Test
    public void testReducer() {

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("122,468,58,131,27,6,13,69,2,2,28,39,0,3,6,4,13"));
        values.add(new Text("153,602,105,189,37,9,27,106,3,1,49,61,5,3,7,4,20"));

        /*
         * For this test, the reducer's input will be "aaronha01 stats1 stats2".
         */
        reduceFunction.withInput(new Text("aaronha01"), values);
        
        /*
         * The expected output is "aaronha01 sumof(stats1, stats2)"
         */
        reduceFunction.withOutput(new Text("aaronha01"),
            new Text("275,1070,163,320,64,15,40,175,5,3,77,100,5,6,13,8,33"));

        /*
         * Run the test.
         */
        reduceFunction.runTest();
    }  

    /*
    * Test the mapper and reducer working together.
    */
    @Test
    public void testMapReduce() {

        // header
        mrFunction.withInput(new LongWritable(1), new Text(BATTING_HEADER));
        // first player
        mrFunction.withInput(new LongWritable(2), new Text(BATTING_DATA[0]));
        mrFunction.withInput(new LongWritable(3), new Text(BATTING_DATA[1]));
        mrFunction.withInput(new LongWritable(4), new Text(BATTING_DATA[2]));
        mrFunction.withInput(new LongWritable(5), new Text(BATTING_DATA[3]));
        mrFunction.withInput(new LongWritable(6), new Text(BATTING_DATA[4]));
        mrFunction.withInput(new LongWritable(7), new Text(BATTING_DATA[5]));
        // second player
        mrFunction.withInput(new LongWritable(8),  new Text(BATTING_DATA[6]));
        mrFunction.withInput(new LongWritable(9),  new Text(BATTING_DATA[7]));
        mrFunction.withInput(new LongWritable(10), new Text(BATTING_DATA[8]));
        mrFunction.withInput(new LongWritable(11), new Text(BATTING_DATA[9]));
        mrFunction.withInput(new LongWritable(12), new Text(BATTING_DATA[10]));
        mrFunction.withInput(new LongWritable(13), new Text(BATTING_DATA[11]));
        mrFunction.withInput(new LongWritable(14), new Text(BATTING_DATA[12]));
        mrFunction.withInput(new LongWritable(15), new Text(BATTING_DATA[13]));
        mrFunction.withInput(new LongWritable(16), new Text(BATTING_DATA[14]));
        mrFunction.withInput(new LongWritable(17), new Text(BATTING_DATA[15]));
        mrFunction.withInput(new LongWritable(18), new Text(BATTING_DATA[16]));
        mrFunction.withInput(new LongWritable(19), new Text(BATTING_DATA[17]));
        mrFunction.withInput(new LongWritable(20), new Text(BATTING_DATA[18]));
        mrFunction.withInput(new LongWritable(21), new Text(BATTING_DATA[19]));
        mrFunction.withInput(new LongWritable(22), new Text(BATTING_DATA[20]));
        mrFunction.withInput(new LongWritable(23), new Text(BATTING_DATA[21]));
        mrFunction.withInput(new LongWritable(24), new Text(BATTING_DATA[22]));
        mrFunction.withInput(new LongWritable(25), new Text(BATTING_DATA[23]));
        mrFunction.withInput(new LongWritable(26), new Text(BATTING_DATA[24]));
        mrFunction.withInput(new LongWritable(27), new Text(BATTING_DATA[25]));
        mrFunction.withInput(new LongWritable(28), new Text(BATTING_DATA[26]));
        mrFunction.withInput(new LongWritable(29), new Text(BATTING_DATA[27]));
        mrFunction.withInput(new LongWritable(30), new Text(BATTING_DATA[28]));
        /*
         * The expected output (from the reducer) is:
         * aardsda01 careerStats
         * aaronha01 careerStats
         */
        mrFunction.withOutput(new Text("aardsda01"),
            new Text("254,3,0,0,0,0,0,0,0,0,0,1,0,0,1,0,0"));
        mrFunction.withOutput(new Text("aaronha01"),
            new Text("3298,12364,2174,3771,624,98,755,2297,240,73,1402,1383,293,32,21,121,328"));

        /*
         * Run the test.
         */
        mrFunction.runTest();
    }       

}
