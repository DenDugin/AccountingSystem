package main;

import Table.Field;
import Table.TableFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Den
 */
public class CreateCollectionTest {
    
    public CreateCollectionTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of FillList method, of class CreateCollection.
     */
    
    @Test
    public void testGetRules() {
 
        List<String> expResult = Arrays.asList( new String[]{"    Field(\"Type\") == \"Type1\"     ",
        "  (Field(\"Type\") == \"Type1\" || Field(\"Type\") == \"Type2\") && Field(\"Height\") == \"Little\"    "
        ,"    Field(\"Type\") == \"Type3\"  &&  Field(\"Weight\") != \"Tall\"  "});

        List<String> result = CreateCollection.GetRules();

        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }
    
    
    
    @Test
    public void testFillList() {
 
         List<Animals> ListAnimals = new ArrayList<>(); 
        
         List<Field> fields = new ArrayList<>();   
         
         fields.add(new Field<String>( "Name", String.class));
         fields.add(new Field<String>( "Weight", String.class));
         fields.add(new Field<String>( "Height", String.class));
         fields.add(new Field<String>( "Type", String.class));
        
         
         
        TableFields AnimalsFields = new TableFields(fields);
         
        List<String> p = Arrays.asList( new String[]{"Mouse",
                                                     "Light",
                                                     "Little",
                                                     "Type1"});
         
        ListAnimals.add( new Animals(AnimalsFields, p.toArray()));
  
        List<Animals> result = CreateCollection.FillList();
        
        
        assertEquals(ListAnimals.get(0).recordFields.toString(), result.get(0).recordFields.toString());

    }

    
}
