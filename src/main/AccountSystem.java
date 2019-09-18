package main;

import java.util.List;
import static Table.TableOperation.*;




public class AccountSystem {
    public static void main( String[] args){

        
        CreateFileRes();

    	List<Animals> LA = CreateCollection.FillList();  // загружаем данные в коллекцию


        
        // сопосб подсчета кол-ва записей с помощью предиката   
        // CreateTable().SelectFrom(LA).Where(r -> r.Field("Name").equals("Mouse")).PrintResult();

        
        // с помощью библиотеки mvel и предиката
        List<String> Rules = CreateCollection.GetRules();   // получам список правил
  

        for (int i=0; i<Rules.size();++i)
        {

       System.out.println("Query № "+ (i+1) + " = " + Rules.get(i));
       
       AddToFile("Query № "+ (i+1) + " = " + Rules.get(i));
       
       
       // Выполнить запрос с выводом полной информации
       CreateTable().SelectFrom(LA).Where(Rules.get(i)).PrintResult();
       
       // Выполнить запрос с выводом только колличеста записей по правилу 
      // CreateTable().SelectFrom(LA).WhereCount(Rules.get(i)).PrintRezult();
      
        }   
        
        CloseFile();
      
        
      
}
}
