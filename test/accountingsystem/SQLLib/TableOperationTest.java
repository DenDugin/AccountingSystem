/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accountingsystem.SQLLib;

import SQLLib.Field;
import SQLLib.Record;
import SQLLib.TableFields;
import SQLLib.TableOperation;
import static SQLLib.TableOperation.CreateTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import main.Animals;
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
public class TableOperationTest {
    
    public TableOperationTest() {
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


    @Test
    public void testCreateTable() {
        
        TableOperation result = TableOperation.CreateTable();
        
        assertNotNull(result);
        // TODO review the generated test code and remove the default call to fail.
        
    }
    
    
    
    @Test
    public void testSelectFrom() {
  
        
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
       

        TableOperation result = CreateTable().SelectFrom(ListAnimals);
        
        assertNotNull(result);
        
        //assertEquals(CreateTable().SelectFrom(ListAnimals), result);

    }
    
    
    @Test
    public void testWhere_String() {
        
        
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
        
        
        TableOperation result = CreateTable().SelectFrom(ListAnimals).Where("Field(\"Name\") == \"Mouse\"");
       
       
       Stream<? extends Record> CountStream = result.table.stream();

       long count = CountStream.count();
        

       assertEquals(count, 1);
        
    }
    
    
    
    
    @Test
    public void testWhere_Predicate() {
        
        
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
         
        
        TableOperation result = CreateTable().SelectFrom(ListAnimals).Where(r -> r.Field("Name").equals("Mouse"));
        
        
        Stream<? extends Record> CountStream = result.table.stream();

        long count = CountStream.count();
 
        assertEquals(count, 1);
   
    }

    /**
     * Test of Where method, of class TableOperation.
     */


    /**
     * Test of WhereCount method, of class TableOperation.
     */
    @Test
    public void testWhereCount() {
      
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
                                                     "Type1"} );
         
        
        ListAnimals.add( new Animals(AnimalsFields, p.toArray()));
        ListAnimals.add( new Animals(AnimalsFields, p.toArray()));
         
        TableOperation result = CreateTable().SelectFrom(ListAnimals);
       
        result.CreateFileRez();
        
        //Stream<? extends Record> CountStream = result.cursorStream; 
       
        result = result.WhereCount("Field(\"Name\") == \"Mouse\"");
 
        long count = result.count;
  
        assertEquals(count, 2);
        
        
    }


 
  
    /**
     * Test of addTextException method, of class TableOperation.
     */
    @Test(expected = SQLLib.TableOperation.StreamSQLException.class)
    public void testAddTextException() {

        TableOperation O =  CreateTable();
       
        O.textExceptionAlreadyAdded = false;
        O.QueryText = "Test Query";
        
        O.addTextException();
        
        O.Where("ss");
        

    }
    
}
