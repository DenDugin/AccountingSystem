package main;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import SQLLib.Field;
import SQLLib.TableFields;

/* 
 * Этот демонстрационный класс организует наполнение данными коллекции Provider.
 */


public class DB {
	public static List<Provider> fillListProvider( String... fieldNames){
            
            
		List<Provider> m = new ArrayList<>( );    // инициализируем коллекцию, в которой будут лежать записи типа Provider

//		TableFields providerFields = // создаем шапку коллекции (аналог полей таблицы) 
//		new TableFields( Stream.of(   // организуем Stream на основе полей шапки
//						 new Field<Integer>( "provider_id", Integer.class),  
//						 new Field<String>( "provider_name", String.class ),
//						 new Field<Integer>( "provider_rating", Integer.class ),
//						 new Field<String>( "provider_city", String.class ))
//				.collect(Collectors.toList())   // получаем List<Field> полей шапки
//				);    
		
                
                List<Field> fields = new ArrayList<>();
                
                fields.add(new Field<String>( "Name", String.class ));
                fields.add(new Field<String>( "Weight", String.class ));
                fields.add(new Field<String>( "Height", String.class ));
                fields.add(new Field<String>( "Type", String.class ));
                
                TableFields providerFields = new TableFields(fields);
                
                
                
		// --- добавляем записи в коллекцию ---------------
//		m.add( new Provider( providerFields, 1, "Иванов", 50, "Прстоквашино"));  
//		m.add( new Provider( providerFields, 2, "Петров", 45, "Прстоквашино"));
//		m.add( new Provider( providerFields, 5, "Сидоров", 40, "Прстоквашино"));
//		m.add( new Provider( providerFields, 0, "Москвашвей", 10, "Москва"));
//		m.add( new Provider( providerFields, 3, "Wrangler", 15, "Европа" ));
//		m.add( new Provider( providerFields, 4, "Puma", 20, "Китай"));


                List<String> Properts = new ArrayList<String>();
                Properts.add("Mouse");
                Properts.add("Light");
                Properts.add("Little");
                Properts.add("Type1");
                
                
               m.add( new Provider(providerFields, Properts));   


		return m;
                
                
                
                
                
                
                
                
                
                
                
	}
	

}
