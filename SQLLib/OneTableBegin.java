package SQLLib;

import java.awt.datatransfer.StringSelection;
import java.lang.reflect.AccessibleObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.StringJoiner;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import main.DB;
import main.Provider;

import java.util.stream.StreamSupport;

/**
 * Данный класс реализует конструкцию 
 * select простые поля, *  
 * from [одна коллекция-таблица]
 */

public class OneTableBegin {
	public boolean trace = false;                  								// надо ли выводить сообщения о ходе выполнения запроса и подробную информацию о стеке ошибки
	public String SQLtext = "";                    								// текстовое представление SQL запроса
	public String SQLname = "";                                                 // имя запроса, необходимо только для отладочной информации
	public Stream<? extends Record> cursorStream;   							// стрим, выполнение которого вернет курсор
	public List<? extends Record> table = new ArrayList<>(); 		  			// копия исходной коллекции-запроcа используемой в запросе (во from)
	public List<Record> cursor;           										// коллекция строк курсора
	public TableFields selectFieldsAliases  = new TableFields();     			// поля коллекций-запросов полученные в курсоре (фактически их алиасы!), представлены именем и лямбдой для получения значения на основе записи (Record) получаемого курсора
	public Map<Field, Function> lambda = new HashMap<>();  						// карта поле-лямбда, лямбда должна возвращать значение поля. Record - вход, ? super T - выход	
	public String[] fields;          											// этот массив нам нужем для начальной фиксации набора полей, которые указаны при вызове from 
    public SelectStep selectStep;   											// шаг в реализации запроса select from        
    public boolean cursorStreamFormed = false;  								// этот флаг обеспечивает выполнение функции formCursorStream не более одного раза
    public boolean textExceptionAlreadyAdded = false;                           // этот флаг обеспечивает добавление в сообщение, информации о SQL запросе не более одного раза 
	public boolean selectFieldsAliasesFormed = false;     						// этот флаг обеспечивает формирование полей курсора не более одного раза

public static void main( String[] args){
    	
    	List<Provider> lM = DB.fillListProvider();    // наполняем коллекцию данными 

    	
    	System.out.println( "\n-------------- исходные данные в коллекции -------------------");
    	lM.stream().forEach(System.out::println);

    	System.out.println( 
    			"\nпервый пример - выполняем"
    			+ "\nselect provider_id AS id," 
    			+ "\n	provider_name as name,"
    			+ "\n	provider_rating as rating," 
   				+ "\n	provider_city as city" 
   			    + "\nfrom lM\n"
    			);


    	select( "provider_id AS id", 
    			"provider_name as name",
    			"provider_rating as rating", 
    			"provider_city as city" 
    			)
    	.setSQLName( "Первый select" )
    	.from( lM )                            
    	.printCursor(true);
    			
    	System.out.println( 
    	    			"\nВторой пример - выполняем используя лямбда ссылки на методы"
    	    			+ "\nselect provider_id," 
    	    			+ "\n	provider_name,"
    	    			+ "\n	provider_rating," 
    	   				+ "\n	provider_city" 
    	   			    + "\nfrom lM\n"
    	    			);

    	select( Provider::getProvider_id, Provider::getProvider_name, Provider::getProvider_rating, Provider::getProvider_city )
    	.setSQLName( "Второй select" )
    	.from( lM )                            
    	.printCursor(true);

    	System.out.println( 
    			"\nтретий пример - как в ORACLE plSQL"
    			+ "\n for rec in ("
    			+ "\n               select "
    			+ "\n                   provider_id id," 
    			+ "\n                   provider_name name"
   			    + "\n               from lM"
   			    + "\n             )"
   			    + "\n   loop"
   			    + "\n         DBMS_OUTPUT.PUT_LINE( 'id='||rec.id||'; name='||rec.name );"
   			    + "\n   end loop;\n"
    			);

    	
    	select( "provider_id id", 
    			"provider_name name" 
    			)
    	.setSQLName( "Третий select" )
    	.from( lM )                            
    	.getCursor()
    	.stream().forEach( System.out::println);
    }
    
    
    
// -------------- создаем объект OneTableBegin и указываем используемые поля из коллекций-таблиц -------------------    
public static OneTableBegin select(String...  fields ){  // список названий полей и алиасов курсора
   	OneTableBegin ot = new OneTableBegin();                     // создаем объект на основе которого функционирует SQL запрос
   	try {
   		ot.setSelectStep( SelectStep.SelectBegin );                                         // отмечаем шаг выполнения Select From
   		ot.SQLtext += "Select " + Stream.of( fields ).collect(Collectors.joining( ", "));   // формируем текстовое представление запроса    	

   		/* только фиксируем поля переданные при вызове select. 
   		* мы не можем преобразовать String в поля, так как нам не изветны их классы (Integer, Long, String), они будут известны, только 
   		* когда определим коллекцию. Мы это сделаем в позже. Аналогично мы не можем пока заменить "*" на список полей
   		*/ 
   		ot.fields = fields;                                                                 // фиксируем список полей переданных при вызове функции
   		ot.printLn("Зафиксированы следующие поля:\n	" +                                     // логируем  
   			Stream.of(fields).collect(Collectors.joining("\n	")) );
    	
   		ot.setSelectStep( SelectStep.SelectEnd );                             // отмечаем шаг выполнения Select From
   	} catch ( Exception e ) { throw ot.new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return ot; 
}

//-------------- второй вариант функции select - указываем используемые поля из коллекций-таблиц как лямбда ссылки на методы -------------------    
public static OneTableBegin select(Function<Provider, Object>...  fields ){  // список названий полей и алиасов курсора
	OneTableBegin ot = new OneTableBegin();                     // создаем объект на основе которого функционирует SQL запрос
	try {
		ot.setSelectStep( SelectStep.SelectBegin );                                         // отмечаем шаг выполнения Select From
		ot.SQLtext += "Select <имя класса><::><имя метода>" ;   // формируем текстовое представление запроса
		
		Stream.iterate( 0, i-> i+1 ).limit( fields.length ).forEach( i->{
							Field field  = new Field( "Filed"+i, fields[i].apply(null).getClass());  // создаем новое поле
							ot.selectFieldsAliases.fields.add( field );          				     // добавляем поле в список полей курсора
							ot.lambda.put( field, fields[i] );  									 // добавляем метод рассчета значения поля в список
		} );
 	    ot.selectFieldsAliasesFormed = true;  
  	    ot.printLn( "В курсоре на основе алиасов созданы следующие поля:	\n	" + ot.selectFieldsAliases.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );
 	
		ot.setSelectStep( SelectStep.SelectEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw ot.new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return ot; 
}

// -------------- определяем коллекцию используемую в запросе и стрим, выполнение которого возвратит курсор -------------------
public OneTableBegin from( List<? extends Record> from ){
   	try {
   		this.setSelectStep( SelectStep.FromBegin );                             // отмечаем шаг выполнения Select From
   		SQLtext += "\nfrom " + from.get(0).getClass().getCanonicalName();       // формируем текстовое представление запроса   
   		table = from;                                                           // фиксируем коллекцию переданную во from для дальнейших действий 
   		if ( from.size() == 0 ) throw new IllegalArgumentException("Не предусмотрено использование пустых коллекций"); 
   		printLn( "Во from используется коллекция записей "+from.get(0).getClass().getCanonicalName()+" с полями:\n	" +  // логируем информацию о переданной во from коллекциии и ее полях 
   														   table.get(0).recordFields.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );

   		cursorStream = table.stream();               // первая инструкция Stream - перебор всех записей коллекции  
    	
   		this.setSelectStep( SelectStep.FromEnd );                             // отмечаем шаг выполнения Select From
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this; 
}

//-------------- формируем шапку курсора -------------------
public void setSelectFieldsAliases(  ) {
	if (selectFieldsAliasesFormed) return;
	// ------ получаем шапку-заголовок коллекции из from
	TableFields tf = table.get(0).recordFields;
 	
	Stream.of(fields )  					// перебираем поля переданные в select()
	.forEach( fs-> {         				
			if ( fs.equals("*"))  {   		// если звездочка, то размножаем строки и получаем список полей
					 tf.fields.stream()     // для каждого поля коллекции-таблицы
					 .forEach( f0-> {
                  			    Field f = new Field( f0.fieldName, f0.cls );    					// создаем поле
			    	 			selectFieldsAliases.fields.add( f );    							// добавляем поле в список курсора
  			  				    lambda.put( f, r-> f.cls.cast(((Record)r).val(f.fieldName)) );  	// добавляем мотод рассчета значения поля в список				  
			     				});
			  }
			  else {                        // если не звездочка а поле с алиасом
				  //отделяем имя поля от алиаса
				  String[] sArr;
				  if ( fs.toUpperCase().contains( " AS ") ) sArr = fs.toUpperCase().split(" AS ");  // парсим строчку, если указан разделитель as
				  else sArr = fs.split(" ");														// парсим строчку, если указан разделитель - пробел
				  sArr[0] = sArr[0].trim();															// удаляем пробелы в названии поля
				  if ( sArr.length == 2 ) sArr[1] = sArr[1].trim();									// удаляем пробелы в названии алиаса
				  if ( sArr.length > 2 ) throw new IllegalArgumentException("Простое поле в select должно задавться в формате ИмяПоля[ [as ИмяАлиаса]], а задано - " + fs );
				  Field f = new Field( (sArr.length==1) ? sArr[0] : sArr[1], tf.getCls(  sArr[0]  ) );  // создаем поле
				  selectFieldsAliases.fields.add( f );          									// добавляем поле в список полей курсора
				  lambda.put( f, r-> f.cls.cast(((Record)r).val(sArr[0])) );  						// добавляем мотод рассчета значения поля в список				  
			  }
		}
	);
 	
	// -------------- проверка: поля курсора должны иметь уникальные имена ------------------------------------------
	String sDouble = selectFieldsAliases.fields.stream()     				// получаем множество полей курсора
		 .collect( Collectors.groupingBy( f -> new String(f.fieldName))) 	// группируем по именам полей
		 .entrySet().stream()                 								// получаем стрим сгруппированных полей
		 .filter(e->e.getValue().size()>1)    								// отфильтровываем поля, с уникальным именем
		 .map( e->e.getKey())												// получаем неуникальное имя поля 
		 .collect( Collectors.joining("\n	"));                            // объединяем в одну строчку все неуникальные имена полей
	if ( sDouble.length() > 0 ) throw new IllegalArgumentException("В курсоре задублированы имена алиасов:\n	" + sDouble);  // генерим ошибку, если есть неуникальные поля   
	
	printLn( "В курсоре на основе алиасов созданы следующие поля:	\n	" + selectFieldsAliases.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );    
}	

// -------------- запускаем на выполнение stream, и возвращаем курсор -------------------------- 
public List<Record> getCursor(  ){
   	try {
   		this.setSelectStep( SelectStep.GetCursorBegin );                             // отмечаем шаг выполнения Select From
    		
    	formCorsorStream( );  														// окончательное оформление Stream tableStream 
    
		cursor = this.cursorStream.collect( Collectors.toList());                   // выполняем cursorStream для получения курсора
		this.setSelectStep( SelectStep.GetCursorEnd );                              // отмечаем шаг выполнения Select From
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this.cursor;
}
    
/* ------------------ окончательное оформление Stream tableStream ------------------------------------------
 * ---------- у нас имеется Stream, который возвращает Record со списком полей коллекции, указанной во from
 * ---------- а нам теперь надо перейти к Record со списком полей, указанных Select
 * ---------- также здесь формируем шапку (поля) курсора
*/
public OneTableBegin formCorsorStream(  ){
 	try {
   		if	( !cursorStreamFormed ) {	
    		cursorStream = cursorStream.map( r-> RecordFactory( r ));   	// переход от записи коллекции к записи курсора
		setSelectFieldsAliases(  ); 										// формируем поля курсора
		cursorStreamFormed = true;
   		}
    }	catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return this;
}


/* ---------- создаем record курсора (с алиасами, указанными в Select) на основе record коллекции, указанной во from */  
public Record RecordFactory(Record r ) {
	Record rec = null; 
	try {
		int fieldCount = selectFieldsAliases.fields.size();   					// количество полей в курсоре
		Object[] fieldsValues = new Object[fieldCount] ;    					// массив, в которолм будут храниться значения полей нового record
	
		Stream.iterate(0, i->i+1).limit(fieldCount)                             // с каждым полем курсора
			.forEach( i -> { Field f = selectFieldsAliases.fields.get( i );     // получаем поле
							 fieldsValues[i] =  lambda.get( f ).apply(r);       // получаем значение поля
							});
		rec = new Record( selectFieldsAliases, fieldsValues);                   // формируем запись курсора 		
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return rec;
}
    
// -------------- выводим на печать то, что вернул запрос -------------------    
public List<Record> printCursor( boolean prettyPrint ){
     if ( prettyPrint ) {	// если запрошена приятная печать результата
    	// --------------- выполняем Stream и получаем реальный список записей курсора ---------------- 
    	List<Record> l = getCursor();    
    	
    	// --------------- инициируем массив значений ширины колонок значениями длинны имени поля ----------------    	
    	Integer[] maxSize =  this.selectFieldsAliases.fields.stream().map( f-> f.fieldName.length()).toArray(Integer[]::new);  // массив значений ширины колонок, заполняем изначально длинной имени поля
    	
    	// ------ корректируем ширину колонок таблицы, чтобы в ней поместилось значение поля любой записи курсора -------------- 
    	l.stream()   // для каждой строки курсора
    	.forEachOrdered( r-> Stream.iterate( 0, i->i+1 ).limit(maxSize.length)  //для каждого поля курсора 
    					 .forEachOrdered( i-> maxSize[i] = Math.max(maxSize[i], r.asStr(r.recordFields.fields.get(i).fieldName).length()))  // если длинна String выражения значения поля больше, то увеличиваем ширину колонки 
  				    );

    	// собираем формат для вывода записи курсора, что-то вроде "| %-<ширина колонки1>.<ширина колонки1>s | %-<ширина колонки2>.<ширина колонки2>s |\n   ----------------------
    	String fieldsFormat = Stream.of( maxSize )  							// для каждого поля курсора
	    		    .map( i->i.toString())  									// переводим размер ширины поля в String 
	    			.reduce("| ", (s,s1)->s+"%-"+ s1+"."+ s1+"s | ")+ "\n"; 	// определяем правило собирания результирующей строки - флрмата вывода полей запсиси
    	
    	// собираем линию разделитель выводимую между записями курсора, что-то вроде +----+----------+--------+
    	String lineFormat =
    			"+-" +                                         // начинаем разделительную линию 
    			Stream.of( maxSize )                           // для каждого поля курсора берем вычисленную для него ширину
    			//.parallel()								   // раскомментарить для параллельного вычисления, чтобы получить ошибку <Эту операцию нельзя выполнять параллельно>	
				.collect( Collector.of(						   // создаем собственный коллектор	
    					StringBuilder::new,					   // сюда будем собирать линию разделитель	
    					( r, w )-> { char[] arr = new char[w]; Arrays.fill( arr , '-'); if ( r.length()==0) r.append(arr); else r.append("-+-").append(arr); },        // получам строку заполненную символов '-' и длинной равной ширигне колонки 
    					//( r1, r2 ) -> r1.append(r2).append("-+-"),           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
    					( r1, r2 ) -> {throw new IllegalArgumentException("Эту операцию нельзя выполнять параллельно!");},           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
    					StringBuilder::toString                // заключительная операция - переводим StringBuilder в String
    			)) +
//    			.collect( StringBuilder::new,					   // сюда будем собирать линию разделитель	
//    					  ( r, w )-> { char[] arr = new char[w]; Arrays.fill( arr , '-'); if ( r.length()==0) r.append(arr); else r.append("-+-").append(arr); },        // получам строку заполненную символов '-' и длинной равной ширигне колонки 
//    					  ( r1, r2 ) -> {}//r1.append(r2).append("-+-")           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
//    			).toString() + 
    			"-+";                                          // завершаем разделительную линию

    	System.out.println( "\nВ результате выполнения запроса \n" + this.SQLtext + "\nполучено:");
    	// --------------- собственно вывод содержимого таблицы на консоль -------------------------------
	    System.out.println( lineFormat);      // выводим верхнюю линию таблицы
	    System.out.printf( fieldsFormat, this.selectFieldsAliases.fields.stream().map( f-> f.fieldName ).toArray( String[]::new));  // выводим названия полей курсора
	    System.out.println( lineFormat);      // выводим двойную разделительную линию, меду шапкой и телом таблицы
	    System.out.println( lineFormat);
	    l.stream().forEachOrdered( r->System.out.printf( fieldsFormat  + lineFormat + "\n", r.fieldsValues ));  // выводим значения строки куросора и разделительную линию
      }
      else   // если запрошена простая печать результата
      {
    	  formCorsorStream( );  														// окончательное оформление Stream tableStream 
   		  this.cursorStream.forEach(System.out::println);								// выводим записи на консоль
      }
   	return this.cursor;
}
    
    
    // -------------  этап в реализации select from -------------------   
    enum  SelectStep{ SelectBegin, SelectEnd, FromBegin, FromEnd, WhereBegin, WhereEnd, 
    	              GroupByBegin, GroupByEnd, HavingBegin, HavingEnd, 
    	              UnionBegin, UnionEnd, MinusBegin, MinusEnd, IntersectionBegin, IntersectionEnd,
    	              OrderByBegin, OrderByEnd,
    	              GetCursorBegin, GetCursorEnd }
    
    // утилита по логированию сообщений на консоль на основе System.out.println  
    public void printLn( String s ) {
    	if (trace) System.out.println( selectStep + "-->" + s);
    }

    /** добавляемый текст к сообщению об ошибке */
    public String addTextException(){
      if ( textExceptionAlreadyAdded ) return "";
      textExceptionAlreadyAdded = true;
      return "\nПри обработке запроса " + ((SQLname == "")? "\n" : "<"+SQLname + ">\n") + this.SQLtext + 
    		  "\nна этапе " + this.selectStep + 
    		  "\nвыявлена ошибка:\n";	
    }
    
    public class StreamSQLException extends RuntimeException {
    	StreamSQLException( String s,  StackTraceElement[] ste ){
    		super( addTextException() + s);
    		this.setStackTrace( ste );
    	}
    	
    	StreamSQLException( String s ){
    		super( addTextException() + s);
    	}
    	
    }
    
    /** можно присвоить имя запросу для использования при отладке */
    public OneTableBegin setSQLName( String SQLname ){
    	this.SQLname = SQLname;
    	return this;
    }
    
    /** при смене шага выполнения select .. from .. проводим проверки правильной последовательности шагов */     
    public void setSelectStep( SelectStep ss ) {
    	if ( this.selectStep != null ) {  // осуществляем проверки последовательности шагов формирования select from 
     	   if (this.selectStep.name().endsWith("End") && ss.name().endsWith( "Begin" ) && (ss.ordinal() - this.selectStep.ordinal() == -1)) 
               throw new IllegalArgumentException( "операция " + this.selectStep + " выполнена и ее нельзя начинать повторно. (вероятно " + 
            		   this.selectStep.toString().substring(0, 1).toLowerCase() + this.selectStep.toString().substring(1, this.selectStep.toString().length() - 3) + " вызвали повторно)."  );
     	   if (this.selectStep.ordinal() >= ss.ordinal() ) 
     		   throw new IllegalArgumentException( "операция " + ss + " должна предшествовать операции " + this.selectStep + 
     				   ". (вероятно " + ss.toString().substring(0, 1).toLowerCase() + ss.toString().substring(1, ss.toString().length() - 5) + 
     				   " вызвали после " + this.selectStep.toString().substring(0, 1).toLowerCase() + this.selectStep.toString().substring(1, this.selectStep.toString().length() - 3) + ")." );
     	   if (this.selectStep.name().endsWith("Begin") && ss.name().endsWith( "Begin" ) ) 
               throw new IllegalArgumentException( "операция " + this.selectStep + " еще не закончилась. Нельзя начинать операцию " + ss );
     	   if (this.selectStep.name().endsWith("End") && ss.name().endsWith( "End" ) ) 
               throw new IllegalArgumentException( "операция " + ss + " заканчивается еще не начавшись" );
    	} 	   
       this.selectStep = ss;
       printLn( "-----------------------------" + this.selectStep.toString() + "-----------------------------------");
    }
    
}





