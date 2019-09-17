package Table;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.swing.JOptionPane;
import org.mvel2.MVEL;

/**
 * Данный класс реализует выборку 
 * из коллекции по указанному в файле Rules.xml набору правил
 * 
 */

public class TableOperation {

	public String QueryText = "";                    			// текстовое представление запроса(правила) к коллекции
	public Stream<? extends Record> cursorStream;   			// стрим, выполнение которого вернет курсор
	public List<? extends Record> table = new ArrayList<>(); 		// копия исходной коллекции-запроcа используемой в запросе (во SelectFrom)
        public boolean textExceptionAlreadyAdded = false;                       // этот флаг обеспечивает добавление в сообщение, информации о SQL запросе не более одного раза       
        private static String FileResult;                                       // резуьтат в файле Result.txt
        private static BufferedWriter writer;      
        public long count;                                                      // кол-во записей в результате запроса(правила) 
       

    
// where с предикатом 
public TableOperation Where( Predicate<Record> predicate  ){
   	try {

    	this.cursorStream = this.cursorStream.filter(predicate);  		// добавляем в Stream курсора предикат - реализуем where
    	
        Stream<? extends Record> CountStream = table.stream();

        count = CountStream.filter(predicate).count();

        System.out.println("Result count = " + count);
                
   	} catch ( Exception e ) { throw new StreamException( e.getMessage(), e.getStackTrace());}
   	return this;
}

// Реализация WHERE с помощью  MVEL
public TableOperation Where( String Rule ){
   	
    try {
        
        QueryText = Rule; 

        Serializable expr = MVEL.compileExpression(Rule); // с помощью библиотеки mvel создаем требуемое правило

        this.cursorStream = this.cursorStream.filter(e->MVEL.executeExpression(expr,e,boolean.class));  // реализуем where с помощью предиката
                                  		
   	} catch ( Exception e ) { throw new StreamException( e.getMessage(), e.getStackTrace());}
   	return this;
}

// Реализуем только Count с помощью MVEL
public TableOperation WhereCount ( String Rule ) {

      try {
        
        QueryText = Rule; 

        Serializable expr = MVEL.compileExpression(Rule);
        
        count = this.cursorStream.filter(e->MVEL.executeExpression(expr,e,boolean.class)).count();
        
        writer.append("Result count = " + count);
        writer.newLine();
        writer.newLine();

                                  		
   	} catch ( Exception e ) { throw new StreamException( e.getMessage(), e.getStackTrace());}
   	return this;  
    
    
}    


public static void CreateFileRes()
{
 try {   
     
 FileResult  =  System.getProperty("user.dir") + "/Result.txt";    
 
 FileWriter fw = new FileWriter(FileResult, false);   
 
 fw.close();
 
 writer = new BufferedWriter(new FileWriter(FileResult, true)); 
  
 } catch ( Exception e )
     
 {
 System.out.println(e.getMessage());
 JOptionPane.showMessageDialog(null,"Ошибка при создании файла " + FileResult + " : " + e.getMessage()); 
 }
 
}  


public static void CloseFile()
{
 try {
     writer.close();
     
 }  catch ( Exception e ) 
 {
 e.getMessage();
 System.out.println(e.getMessage());
 JOptionPane.showMessageDialog(null,"Ошибка при закрытии файла " + FileResult + " : " + e.getMessage()); 
 }
 
}        
        
    

public static void AddToFile(String Data)
{
 try {
     
    writer.append(Data);
    writer.newLine();
     
 }  catch ( Exception e ) 
 {
 System.out.println(e.getMessage());
 JOptionPane.showMessageDialog(null,"Ошибка при добавлении записи в файл " + FileResult + " : " + e.getMessage()); 
 }
 
}     



 public static TableOperation CreateTable()
{
 return new TableOperation();    
}



// определяем коллекцию-таблицу используемую в запросе 
public TableOperation SelectFrom( List<? extends Record> from ) {

try {

        if ( from.size() == 0 ) throw new IllegalArgumentException("Не предусмотрено использование пустых коллекций"); 
        
    	table = from;                                                           // фиксируем коллкцию
    	cursorStream = from.stream();	// Create Stream 			//  создание Stream на основе записей коллекции

	} catch ( Exception e ) { throw new StreamException( e.getMessage(), e.getStackTrace());}
   	return this; 

    
}

    
        
        
        
  public void PrintResult() {      
        
try {      
      
List<Record> l = this.cursorStream.collect( Collectors.toList());                  
if ( l.size()>0 )
{     
    writer.append("Result count = " + l.size());
    writer.newLine();
    writer.append("______________________________\n");

    
    System.out.println("Result count = " + l.size());
    System.out.println("______________________________");
      for (int i=0; i<l.get(0).recordFields.fields.size(); ++i )
    {          
     System.out.print(l.get(0).recordFields.getFieldName(i)+ " | ");  
      writer.append(l.get(0).recordFields.getFieldName(i)+ " | ");      
    }

    writer.newLine();
    writer.append("--------------------------------\n");
    
    System.out.println("");
    System.out.println("--------------------------------");
        

   for (int i=0; i<l.size(); ++i )
    {          
     for (int j=0;j<l.get(i).recordFields.fields.size(); ++j)   
      {  System.out.print(l.get(i).Field(table.get(i).recordFields.fields.get(j).fieldName)+" | ");  
         writer.append(l.get(i).Field(table.get(i).recordFields.fields.get(j).fieldName)+" | ");}
         System.out.println(""); 
         writer.newLine();
    }
   
System.out.println("--------------------------------");
writer.append("--------------------------------\n\n\n");


}
else AddToFile("Result count = 0");
 
} catch ( Exception e ) 
{
     throw new StreamException( e.getMessage(), e.getStackTrace());  
}
     
}
        
        

    // добавляемый текст к сообщению об ошибке
    public String addTextException(){
      if ( textExceptionAlreadyAdded ) return "";
      textExceptionAlreadyAdded = true;
      return "\nПри обработке запроса : " + this.QueryText +     		  
    		  "\nвыявлена ошибка:\n";	
    }
    
    public class StreamException extends RuntimeException {
    	StreamException( String s,  StackTraceElement[] ste ){
    		
               super( addTextException() + s);
    		this.setStackTrace( ste );                
                
                AddToFile(addTextException() + s);
                AddToFile(Arrays.toString(ste));
                CloseFile();
     
    	}
    	
    	StreamException( String s ){
    		super( addTextException() + s);
    	}
    	
    }
    
    
}




