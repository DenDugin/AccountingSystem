package Table;


import static Table.TableOperation.CreateTable;
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

    private  List<Animals> ListAnimals;   
    
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
        
        
         ListAnimals = new ArrayList<>();         
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

    }
    
    @After
    public void tearDown() {
        ListAnimals.clear();
    }


    @Test
    public void testCreateTable() {
        
        TableOperation result = TableOperation.CreateTable();
        
        assertNotNull(result);

    }
    
    
    
    @Test
    public void testSelectFrom() {

        List<Animals> LA = new ArrayList<>(ListAnimals); 
    
        TableOperation result = CreateTable().SelectFrom(LA);
        
        assertNotNull(result);

    }
    
    
    @Test
    public void testWhere_String() {
       
       List<Animals> LA = new ArrayList<>(ListAnimals);

       TableOperation result = CreateTable().SelectFrom(LA).Where("Field(\"Name\") == \"Mouse\"");

       Stream<? extends Record> CountStream = result.table.stream();

       long count = CountStream.count();

       assertEquals(count, 1);
        
    }
    
    
    
    
    @Test
    public void testWhere_Predicate() {

        List<Animals> LA = new ArrayList<>(ListAnimals);
        
        TableOperation result = CreateTable().SelectFrom(LA).Where(r -> r.Field("Name").equals("Mouse"));

        Stream<? extends Record> CountStream = result.table.stream();

        long count = CountStream.count();
 
        assertEquals(count, 1);
   
    }

    
    @Test
    public void testWhereCount() {
        
         List<Animals> LA = new ArrayList<>(ListAnimals);
        
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

        LA.add( new Animals(AnimalsFields, p.toArray()));
         
        TableOperation result = CreateTable().SelectFrom(LA);
       
        result.CreateFileRes();
        
        //Stream<? extends Record> CountStream = result.cursorStream; 
       
        result = result.WhereCount("Field(\"Name\") == \"Mouse\"");
 
        long count = result.count;
  
        assertEquals(count, 2);
        
        
    }


 
  
    /**
     * Test of addTextException method, of class TableOperation.
     */
    @Test(expected = Table.TableOperation.StreamException.class)
    public void testAddTextException() {

        TableOperation O =  CreateTable();

        O.textExceptionAlreadyAdded = false;
        
        O.QueryText = "Test Query";
        
        O.addTextException();
        
        O.Where("Test#$");
        

    }
    
}
