/**	
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Class: BattingTest
 * File: BattingTest.java
 * Created: 2013-03-05 21:06:29.205574
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 */
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Description: Tell me about the good-ole days grandpa
 * @author corbett
 */ 
public class BattingTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
    	// Code placed here will be executed before first test method       
    }
 
    @Before
    public void setUp() throws Exception {
        // Code placed here will be executed before each test    
    }
 
    @Test
    public void testOneThing() {
        // Code that tests one thing
        String line = "abbotpa01,2003,1,KCA,AL,10,0,,,,,,,,,,,,,,,,,10";
        String[] columns = line.split(",");
        if(columns.length != 24) {
            fail("should have been 24 columns but only read: " + columns.length);
        }
        if(!"abbotpa01".equals(columns[0])) {
            fail("first column should have been abbotpa01");
        }
        if(!"10".equals(columns[23])) {
            fail("last column should have been 10");
        }
        if(!"".equals(columns[7])) {
            fail("column 7 should be empty String");
        }
    }
 
    @After
    public void tearDown() throws Exception {
        // Code placed here will be executed after each test   
    }
 
    @AfterClass
    public static void tearDownClass() throws Exception {
        // Code placed here will be executed after last test method 
    }
}
