package main;

import java.util.List;
import static Table.TableOperation.*;




public class AccountSystem {
    public static void main( String[] args){
        

   	
        CreateFileRes();

    	List<Animals> LA = CreateCollection.FillList();  // загружаем данные в коллекцию


        
        // сопосб подсчета кол-ва записей с помощью предиката   
        // CreateTable().SelectFrom(LA).Where(r -> r.Field("Name").equals("Mouse")).PrintResult();
         
        // инициализируем в стаиическом методе класс Ote Table Group
        
        
        
        // с помощью либы 
        List<String> Rules = CreateCollection.GetRules();   // получам список правил
  
        //for ( String i : Rules )
        for (int i=0; i<Rules.size();++i)
        {

       System.out.println("Query № "+ (i+1) + " = " + Rules.get(i));
       
       AddToFile("Query № "+ (i+1) + " = " + Rules.get(i));
       
       
       // инициализируем класс TableOperation
       CreateTable().SelectFrom(LA).Where(Rules.get(i)).PrintResult();
       
      // CreateTable().SelectFrom(LA).WhereCount(Rules.get(i)).PrintRezult();
      
       System.out.println("");
       
        }   
        
        CloseFile();
      
        
      
}
}
