/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accountingsystem.main;

import SQLLib.Field;
import SQLLib.TableFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import main.Animals;
import main.CreateCollection;
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
 
        List<String> expResult = Arrays.asList( new String[]{"    Field(\"Type\") != \"Type1\"     ",
        "  (Field(\"Type\") == \"Type1\" || Field(\"Type\") == \"Type2\") && Field(\"Height\") == \"Little\"    "
        ,"    Field(\"Name\") != \"Mouse\"  &&  Field(\"Weight\") == \"Light\"  "});

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
