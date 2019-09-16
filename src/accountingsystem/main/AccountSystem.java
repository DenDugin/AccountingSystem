package main;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import SQLLib.*;
import static SQLLib.TableOperation.*;
import java.io.Serializable;
import static java.lang.Math.*;
import org.mvel2.MVEL;

/* Это демонстрационный класс, в котором представлено использование 
 * запросов select к java коллекции, c разными опциями 
 */

public class AccountSystem {
    public static void main( String[] args){
        

    	
        CreateFileRez();

    	List<Animals> LA = CreateCollection.FillList();  // загружаем данные в коллекцию
    	


//    	System.out.println( "\n 1)--------------------------------------------------------------------------------------------------------\n" +
//    			 			"select *\n" +
//    	                    "from lM\n" );
//    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
//    	select( "*" )                      // вывести в курсор все поля Provider
//    	.from( lM )                        // исходная коллекция lM
//    	.printCursor(false);		   // реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы	
//    	
        
       

        
        // сопосб подсчета кол-ва с помощью предиката
         //select( "1223" ).from( lM ).where( r -> r.Field("Name").equals("Mouse")).printCursor(false);
    	
       //  CreateTable().SelectFrom(lM).Where(r -> r.Field("Name").equals("Mouse")).PrintRezult();
         
        // инициализируем в стаиическом методе класс Ote Table Group
        
        
        
        // с помощью либы 
        List<String> Rules = CreateCollection.GetRules();
  
        //for ( String i : Rules )
        for (int i=0; i<Rules.size();++i)
        {

       System.out.println("Query № "+ (i+1) + " = " + Rules.get(i));
       
       AddToFile("Query № "+ (i+1) + " = " + Rules.get(i));
       
       CreateTable().SelectFrom(LA).Where(Rules.get(i)).PrintRezult();
       
      // CreateTable().SelectFrom(LA).WhereCount(Rules.get(i)).PrintRezult();
      
       System.out.println("");
       
        }   
        
        CloseFile();
      
        
      
}
}
