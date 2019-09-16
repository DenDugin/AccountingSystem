package main;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import SQLLib.*;
import static SQLLib.OneTableGroupBy.*;
import static java.lang.Math.*;

/* Это демонстрационный класс, в котором представлено использование 
 * запросов select к java коллекции, c разными опциями 
 */

public class OneTableTest {
    public static void main( String[] args){
    	
    	List<Provider> lM = DB.fillListProvider();  // загружаем данные в коллекцию
    	

    	System.out.println( "\n-------------- исходник -------------------");
    	lM.stream().forEach(System.out::println);   // выводим на консоль содержимое коллекции lM

    	
    	
    	System.out.println( "\n 1)--------------------------------------------------------------------------------------------------------\n" +
    			 			"select *\n" +
    	                    "from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
    	select( "*" )                      // вывести в курсор все поля Provider
    	.from( lM )                        // исходная коллекция lM
    	.printCursor(true);				   // реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы	
    	
    	
    	
    	System.out.println( "\n 2)--------------------------------------------------------------------------------------------------------\n" +
	 						"select provider_id, provider_name\n" +
                			"from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );

    	select( "provider_id", "provider_name" )    // вывести в курсор только поля provider_id и provider_name
    	.from( lM )									// исходная коллекция lM		
    	.printCursor(true);							// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы

    	
    	System.out.println( "\n 3)--------------------------------------------------------------------------------------------------------\n" +
					"select distinct provider_city\n" +
    				"from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
		select( "provider_city" )					// вывести в курсор только поля provider_city
		.distinct()									// в курсоре останутся только уникальные записис
		.from( lM )									// исходная коллекция lM
		.printCursor(true);							// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы
    	
    	
    	System.out.println( "\n 4)----------- нельзя допускать дубля алиасов полей!!! Сейчас будет ошибка! --------\n" +
    						"select provider_id, provider_name, * \n" +
    						"from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
    	try {
    	select( "provider_id", "provider_name", "*" )    // вывести в курсор только поля provider_id и provider_name и *, то-есть все поля, 
    													 // здесь будет ошибка так как поля provider_id и provider_name будут указаны дважды в курсоре 
    	.from( lM )										 // исходная коллекция lM
    	.printCursor(true);								 // реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы	
    	} catch (Exception e ) {e.printStackTrace();};   

    	try {	Thread.sleep(500);	} catch (InterruptedException e) {	e.printStackTrace();}  // чтобы e.printStackTrace() успел вывестись на консоль
    	
    	System.out.println( "\n 5)------------- чтобы не было дублирования полей в курсоре надо указать алиасы -------------------------\n" +
				"select provider_id id, provider_name name, * \n" +
				"from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
    	select( "provider_id as  id", "provider_name  as name", "*" )   // вывести в курсор только поля provider_id с алиаосм id и provider_name с алиасом name 
    																	// и *, то-есть все поля. Здесь ошибки не будет, так как алиасы полей в курсоре будут уникальны  
    	.from( lM )														// исходная коллекция lM
    	.printCursor(true);												// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы

   	
    	System.out.println( "\n 6)-----------------------------------------------------------------------------------------------------\n" +
				  " select provider_rating provider_rating, \n"
				+ "  	   provider_name name, \n"
				+ " 	   provider_rating*2+7 \"(provider_rating*2+7)\", \n"
				+ " 	   least(provider_id,40) \"min(provider_rating,40)\", \n"
				+ " 	   greatest(provider_id,40) \"another_max(provider_rating,40)\", \n"
				+ " from lM\n" );
    	System.out.println( "--------------------------------------------------------------------------------------------------------" );
    	select( "provider_rating as  provider_rating",                                       // вывести в курсор поле provider_rating
    			"provider_name  as name", 													 // вывести в курсор поле provider_name as name
    			calcInt( r->r.asInt("provider_rating")*2+7, "(provider_rating*2+7)" ),		 // вывести в курсор калькуляционное поле 		
    			calcInt( r->{ Integer i = r.asInt("provider_rating")*2+7; 					 // вывести в курсор калькуляционное поле, на основе сложного кода со многими выражениями
    						  if ( i > 40 ) i = 40;
    						  return i;
    						}
    					 , "min(provider_rating*2+7,12)" ),
    			calcInt( r-> max( r.asInt("provider_rating")*2+7, 40 )                       // вывести в курсор калькуляционное поле. max - это Math.max, но Math можно опустить так как объявлен import static java.lang.Math.*; 
    						, "another_max(provider_rating*2+7,12)" )
    		  )	
    	.from( lM )																			 // исходная коллекция lM
    	.printCursor(true);																	 // реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы 	
    	

  	System.out.println( "\n 7)-----------------------------------------------------------------------------------------------------\n" +
				  " select provider_rating provider_rating, \n"
				+ "  	   provider_name name, \n"
				+ " 	   provider_rating*2+7 \"(provider_rating*2+7)\", \n"
				+ " from lM\n" 
				+ " where provider_rating < 40 \n"
				+ "   and provider_name = 'Puma'" );
	System.out.println( "--------------------------------------------------------------------------------------------------------" );
  	
  	select( "provider_rating as  provider_rating", 											// вывести в курсор поле provider_rating
  			"provider_name  as name", 														// вывести в курсор поле provider_name as name
  			calcInt( r->r.asInt("provider_rating")*2+7, "(provider_rating*2+7)" )			// вывести в курсор калькуляционное поле
  		  )	
  	.from( lM )																				// исходная коллекция lM
  	.where( r -> r.asInt("provider_rating") < 40 											// указываем в where предикат
  			&& r.asStr( "provider_name" ) == "Puma")
  	.printCursor(true);																		// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы


  	System.out.println( "\n 8)-----------------------------------------------------------------------------------------------------\n" +
			  " select provider_rating provider_rating, \n"
			+ "  	   provider_name name, \n"
			+ " 	   provider_rating*2+7 \"(provider_rating*2+7)\", \n"
			+ " from lM\n" 
			+ " where provider_rating < 40 \n"
			+ " order by provider_name");
	System.out.println( "--------------------------------------------------------------------------------------------------------" );

  	select( "provider_rating as  provider_rating", 											// вывести в курсор поле provider_rating
  			"provider_name  as name", 														// вывести в курсор поле provider_name as name
  			calcInt( r->r.asInt("provider_rating")*2+7, "(provider_rating*2+7)" )			// вывести в курсор калькуляционное поле
  			)	
  	.from( lM )																				// исходная коллекция lM
  	.where( r -> r.asInt("provider_rating") < 40 )											// указываем в where предикат
  	.orderBy(" name ")																// реализуем сортировку курсора
  	.printCursor(true);																		// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы
  	

	union (																				    // объединяем содержание курсоров двух запросов
			select( "provider_id as id", 													// список полей, которые надо вывести в курсор у первого запроса
					"provider_name as name",
					"provider_rating as rating", 
					"provider_city as city" 
					)
			.setSQLName( "Первый select" )													// присваиваем название запросу (необходимо только для ифнормационных сообщений)					
			.from( lM )                   													// исходная коллекция lM         
			.where( r->r.asInt("provider_id") >= 2),										// указываем в where предикат
			select( "provider_id as id", 													// список полей, которые надо вывести в курсор у первого запроса
					"provider_name as name",
					"provider_rating as rating", 
					"provider_city as city" 
					)
			.setSQLName( "Второй select" )													// присваиваем название запросу (необходимо только для ифнормационных сообщений)
			.from( lM )                   													// исходная коллекция lM         
			.where( r->r.asInt("provider_id") == 3)											// указываем в where предикат
			)
	  .setSQLName( "Union select" )															// присваиваем название объединенному запросу
	  .printCursor(true)																			// реализуем Stream и получаем список List<Record> 
  	;
  	
	System.out.println( "\n 9)-------------- выполнение запроса -------------------");
	select ( "city", count("id", "cnt") )                                             		// вывести в курсор поля city и агрегированное поле count()
	.from (
		union (																				// объединяем содержание курсоров двух запросов
			select( "provider_id as id", 													// список полей, которые надо вывести в курсор у первого запроса
					"provider_name as name",
					"provider_rating as rating", 
					"provider_city as city" 
					)
			.setSQLName( "Первый select" )													// присваиваем название запросу (необходимо только для ифнормационных сообщений)					
			.from( lM )                   													// исходная коллекция lM         
			.where( r->r.asInt("provider_id") >= 2),										// указываем в where предикат
			select( "provider_id as id", 													// список полей, которые надо вывести в курсор у первого запроса
					"provider_name as name",
					"provider_rating as rating", 
					"provider_city as city" 
					)
			.setSQLName( "Второй select" )													// присваиваем название запросу (необходимо только для ифнормационных сообщений)
			.from( lM )                   													// исходная коллекция lM         
			.where( r->r.asInt("provider_id") == 3)											// указываем в where предикат
			)
	  .setSQLName( "Union select" )															// присваиваем название объединенному запросу
	  .getCursor()																			// реализуем Stream и получаем список List<Record> 
	)
	.setSQLName( "groupBy Select" )															// присваиваем название запросу с group by
	.groupBy( "city" )  																	// производим группировку по полю city
	.printCursor(true);																		// реализуем cursorStream, получаем курсор List<Record> и выводим его содержимое в виде таблицы



  	
}
}
