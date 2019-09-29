package demo;
import static org.junit.Assert.*;

import org.junit.Test;

import demo.DataGenerator;

public class Test03_generate_data {

	@Test
	public void testDataGenerator() {
		new DataGenerator(50 , "resources/GeneratedTestData.json");
		
	}

}
