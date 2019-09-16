package SQLLib;

import java.awt.datatransfer.StringSelection;
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

import SQLLib.OneTableCalc.FieldType;
import main.DB;
import main.Provider;

import java.util.stream.StreamSupport;


/**
 * Данный класс реализует конструкцию 
 * select distinct простые поля, *, калькулируемые поля  
 * from [одна коллекция-таблица]
 * where [предикат]
 * order by
 */

public class OneTableWhere {
		public boolean trace = false;                   								// надо ли выводить сообщения о ходе выполнения запроса и подробную информацию о стеке ошибки
		public String SQLtext = "";                    								// текстовое представление SQL запроса
		public String SQLname = "";                                                 // имя запроса, необходимо только для отладочной информации
		public Stream<? extends Record> cursorStream;   								// стрим, выполнение которого вернет курсор
		public List<? extends Record> table = new ArrayList<>(); 		  						// копия исходной коллекции-запроcа используемой в запросе (во from)
		public List<Record> cursor;           										// коллекция строк курсора
		public TableFields selectFieldsAliases  = new TableFields();     			// поля коллекций-запросов полученные в курсоре (фактически их алиасы!), представлены именем и лямбдой для получения значения на основе записи (Record) получаемого курсора
		public Map<Field, FieldU> lambda = new HashMap<>();  						// карта поле-лямбда, лямбда должна возвращать значение поля. Record - вход, ? super T - выход	
		public Object[] fields;          											// этот массив нам нужем для начальной фиксации набора полей, которые указаны при вызове from 
		public boolean isDistinct = false;  										// надо ли удалять из курсора дубли строк, этот параметр устанавливается в начале конструкции select, но действие применяется в самом конце
	    public String orderByFields;                                                // поля сортировки
	    public SelectStep selectStep;   											// шаг в реализации запроса select from        
	    public boolean cursorStreamFormed = false;  								// этот флаг обеспечивает выполнение функции formCursorStream не более одного раза
	    public boolean textExceptionAlreadyAdded = false;                           // этот флаг обеспечивает добавление в сообщение, информации о SQL запросе не более одного раза 
	    public static void main( String[] args){
	    	
	    	List<Provider> lM = DB.fillListProvider();    // наполняем коллекцию данными 
	    	
	    	System.out.println( "\n-------------- исходные данные в коллекции -------------------");
	    	lM.stream().forEach(System.out::println);
	     
	    	select( "provider_rating as  provider_rating", 
	      			"provider_name  as name" 
	      			)	
	      	.from( lM )
	      	//.orderBy(" provider_name ")
	      	.printCursor(true);
	    	
	    	/*
	    	 * пример соответствует:
	    	select city, count(id) cnt 
	    	from (
	    			select provider_id as id, 
	    					provider_name as name,
	    					provider_rating as rating, 
	    					provider_city as city 
	    			from lM                             
	    			where provider_id = 2 
				union 
	    			select provider_id as id", 
	    					provider_name as name,
	    					provider_rating as rating, 
	    					provider_city as city 
	    			from lM
	    			where provider_id = 3 
	    	)
	    	 */

	    	select( "provider_id AS id", 
	    			"provider_name as name",
	    			"provider_rating as rating", 
	    			"provider_city as city" 
	    			)
	    	.setSQLName( "Первый select" )
	    	.from( lM )                            
	    	.printCursor(true);

	    	select( "provider_id AS id", 
	    			"provider_name as name",
	    			"provider_rating as rating", 
	    			"provider_city as city" 
	    			)
	    	.setSQLName( "Второй select" )
	    	.from( lM )
	    	.where( r ->
	    			r.asInt( "provider_id" ) > 2 ) 
	    	.printCursor(true);

	    	select( "provider_city as city"	)
	    	.setSQLName( "Третий select" )
	    	.from( lM )                            
	    	.printCursor(true);

	    	select( "provider_city as city"	)
	    	.setSQLName( "Четвертый select" )
	    	.from( lM )
	    	.distinct()
	    	.printCursor(true);
	    	
	    	
	    	select ( "id", "name" )
	    	.from(
	    			select( "provider_id AS id", 
	    					"provider_name as name",
	    					"provider_rating as rating", 
	    					"provider_city as city" 
	    					)
	    			.setSQLName( "Пятый select" )
	    			.from( lM )
	    			.where( r ->
	    					r.asInt( "provider_id" ) > 2 )
	    			.getCursor()
	    			)
			.setSQLName( "многоэтажный select" )
	    	.printCursor(true);
	    	
	    	
	    	select( "provider_id AS id", 
	    			"provider_name as name",
	    			"provider_rating as rating", 
	    			"provider_city as city" 
	    			)
	    	.setSQLName( "Запрос с использованием в where подзапроса" )
	    	.from( lM )
	    	.where( r -> r.asStr( "provider_city" ) ==
	    				( select( "provider_city" )  
	    				  .from( lM )
	    			      .where(r1 -> r1.asInt( "provider_id" ) == 1 )
	    			      .getCursor().get(0).asStr( "provider_city" )
	    			     )
	    	)
	    	.printCursor(true);
	    }
	    

/** -------------- реализуем orderby ------------------*/ 
public OneTableWhere orderBy( String fields ){    										// список полей сортировки, перечисленных, через запятую
    try {
    	this.setSelectStep( SelectStep.OrderByBegin );                             			// отмечаем шаг выполнения Select From
    	SQLtext += "\norder by " + fields;  												// добавляем шаг order by в текстовое представление запроса
	    	
    	orderByFields = fields;                                                             // здесь только фиксируем список имен файлов сортировки   
	    
		this.setSelectStep( SelectStep.OrderByEnd );                             			// отмечаем шаг выполнения Select From
    } catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
    return this;
}
	    
// -------------- where с одним предикатом -------------------------- 
public OneTableWhere where( Predicate<Record> predicate  ){
   	try {
   		this.setSelectStep(  SelectStep.WhereBegin );                             // отмечаем шаг выполнения Select From
    	SQLtext += "\nwhere <предикат>";

    	this.cursorStream = this.cursorStream.filter(predicate);  // добавляем в Stream курсора предикат - реализуем where
	    	
		this.setSelectStep( SelectStep.WhereEnd );                             // отмечаем шаг выполнения Select From
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this;
}

// -------------- запускаем на выполнение stream, возвращающий курсор -------------------------- 
public OneTableWhere distinct(  ){
   	isDistinct = true;                             // здесь только запоминаем, что была затребована опция distinct, а реализуем ее в самом конце, в formCorsorStream()
   	return this;
}

//-------------- создаем объект OneTableWhhere и указываем используемые поля из коллекций-таблиц -------------------    
public static OneTableWhere select(Object...  fields ){  // список названий полей и алиасов курсора или объекты FieldU (калькулируемое поле)
	OneTableWhere ot = new OneTableWhere();                     // создаем объект на основе которого функционирует SQL запрос
   	try {
    	ot.setSelectStep( SelectStep.SelectBegin );                             // отмечаем шаг выполнения Select From
    	ot.SQLtext += "Select " + Stream.of( fields )                    		// формируем текстовое представление запроса
	    			.map( f -> {if ( f instanceof String ) return (String)f;	// если передано имя поля с алиасом 
	    						else if (f instanceof FieldU) return ( ((FieldU)f).fieldType + " " + ((FieldU)f).f.fieldName );  // если передано калькулируемое поле
     							else throw new IllegalArgumentException("В select нельзя передавать поля как объекты класса " + f.getClass().getCanonicalName()); 
	    						})
	    			.collect(Collectors.joining( ", "));    	

   		/* только фиксируем поля переданные при вызове select. 
   		* мы не можем преобразовать String в поля, так как нам не изветны их классы (Integer, Long, String), они будут известны, только 
   		* когда определим коллекцию. Мы это сделаем в позже. Аналогично мы не можем пока заменить "*" на список полей
   		*/ 
	    ot.fields = fields;
	    ot.printLn("Зафиксированы следующие поля:\n	" +  
	    			Stream.of(fields).map( o-> { if (o instanceof String) return (String)o; 
	    			   							 else if (o instanceof FieldU) return ((FieldU)o).toString();
	    			   							 else throw new IllegalArgumentException("В select нельзя передавать поля как объекты класса " + o.getClass().getCanonicalName()); })
	    			.collect(Collectors.joining("\n	")) );
	    	
    	ot.setSelectStep( SelectStep.SelectEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw ot.new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return ot; 
}

//-------------------- добавляем калькулируемое поле типа Integer-------------------
public static FieldU<Integer> calcInt( Function<Record, Integer> lambda, String calcFieldName ){  // список названий полей, которые дают значения для курсора (но не алиасы этих полей)
	return new FieldU<Integer>(new Field<Integer>( calcFieldName, Integer.class ), FieldType.Calc, lambda, null);		
}
//-------------------- добавляем калькулируемое поле типа Long-------------------
public static FieldU<Long>  calcLong( Function<Record, Long> lambda, String calcFieldName ){  // список названий полей, которые дают значения для курсора (но не алиасы этих полей)
	return new FieldU<Long>(new Field<Long>( calcFieldName, Long.class ), FieldType.Calc,  lambda, null);		
}

//-------------------- добавляем калькулируемое поле типа String-------------------
public static FieldU<String> calcStr( Function<Record, String> lambda, String calcFieldName ){  // список названий полей, которые дают значения для курсора (но не алиасы этих полей)
	return new FieldU<String>(new Field<String>( calcFieldName, String.class ), FieldType.Calc,  lambda, null);
}

//-------------------- добавляем простое поле -------------------
public static String simple( String fieldName, String aliasName ){  // список названий полей, которые дают значения для курсора (но не алиасы этих полей)
	return ((fieldName=="*")? "*" : fieldName+ " as " + aliasName);
}

//-------------- определяем коллекцию-таблицу используемую в запросе и стрим, выполнение которого возвратит курсор -------------------
public OneTableWhere from( List<? extends Record> from ){
	try {
		this.setSelectStep( SelectStep.FromBegin );                             // отмечаем шаг выполнения Select From
		SQLtext += "\nfrom " + from.get(0).getClass().getCanonicalName();       // формируем текстовое представление запроса   
		table = from;                                                           // фиксируем коллекцию переданную во from для дальнейших действий 
		if ( from.size() == 0 ) throw new IllegalArgumentException("Не предусмотрено использование пустых коллекций"); 
		printLn( "Во from используется коллекция записей "+from.get(0).getClass().getCanonicalName()+" с полями:\n	" +  // логируем информацию о переданной во from коллекциии и ее полях
	    			from.get(0).recordFields.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );

		// ------ создаем шапку-заголовок коллекции-таблицы  
	    TableFields tf = from.get(0).recordFields;

		cursorStream = table.stream();               // первая инструкция Stream - перебор всех записей коллекции  
	    	
	    	
		this.setSelectStep( SelectStep.FromEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return this; 
}

//-------------- формируем шапку курсора -------------------
public void setSelectFieldsAliases( ) {
	// ------ создаем шапку-заголовок коллекции-таблицы из from
	TableFields tf = table.get(0).recordFields;
	 	
	Stream.of(fields )  						// перебираем поля переданные в select()
		.forEach( o-> {        					// переходим от Object к FiledU или String и заполняем списки полей курсора
			if (o instanceof String) {          // если передали String
				  String fs = (String)o;   		// если в select обозначили поле через строку
				  if ( fs.equals("*"))  {   	// если звездочка, то размножаем строки и получаем список полей
						 tf.fields.stream()    	// для каждого поля коллекции
						 .forEach( f0-> {
	                  			    Field f = new Field( f0.fieldName, f0.cls );                   // создаем поле
				    	 			selectFieldsAliases.fields.add( f );                           // добавляем поле в список курсора
				    	 			lambda.put( f, new FieldU( f, FieldType.Simple, r-> f.cls.cast(((Record)r).val(f.fieldName)), null ) ); // добавляем мотод рассчета значения поля в список
				     				});
				  }
				  else {			// если не звездочка а поле с алиасом
					  //отделяем имя поля от алиаса
					  String[] sArr;
					  if ( fs.toUpperCase().contains( " AS ") ) sArr = fs.toUpperCase().split(" AS ");  // парсим строчку, если указан разделитель as
					  else sArr = fs.split(" ");														// парсим строчку, если указан разделитель - пробел
					  sArr[0] = sArr[0].trim();															// удаляем пробелы в названии поля
					  if ( sArr.length == 2 ) sArr[1] = sArr[1].trim();									// удаляем пробелы в названии алиаса
					  if ( sArr.length > 2 ) throw new IllegalArgumentException("Простое поле в select должно задавться в формате ИмяПоля[ [as ИмяАлиаса]], а задано - " + fs );
	    			  else {
	    				  Field f = new Field( (sArr.length==1) ? sArr[0] : sArr[1], tf.getCls(  sArr[0]  ) );   						 // создаем поле
	    				  selectFieldsAliases.fields.add( f );         																	 // добавляем поле в список полей курсора
	    				  lambda.put( f, new FieldU( f, FieldType.Simple, r-> f.cls.cast(((Record)r).val(sArr[0])),  null ) ); 	 // добавляем мотод рассчета значения поля в список
	    			  }
				  }
			}
			else if (o instanceof FieldU) {                         // если передали FieldU
				selectFieldsAliases.fields.add( ((FieldU)o).f );    // добавляем поле в список полей курсора
				lambda.put( ((FieldU)o).f, (FieldU)o );             // добавляем мотод рассчета значения поля в список
			}
		});
	 	
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
	    		
    	formCorsorStream();  														 // окончательное оформление Stream tableStream 

    	if ( orderByFields != null ) {  											 // если была задана сортировка
    		// если была затребована сортировка и в списке полей полей, перечисленных в order by есть поле, которого нет в полях курсора, то генерим ошибку
	    	Stream.of( orderByFields.split(",") )   								// -------------- получаем стрим <поле сортировки[ desc]>  -----------------------
   				.map( s->s.trim().split(" ")[0].trim() )  							// отрезаем пробелы справа и слева от каждого поля сортировки и отрезаем desc
   				.forEach( s->                										// с каждым полем сортировки
   					selectFieldsAliases.fields.stream().filter( f-> (f.fieldName.compareToIgnoreCase( s )== 0))  // фильтр пропустит из полей курсора только поля, совпадающие с полем сортировки 
   					.findFirst().orElseThrow(()->new IllegalArgumentException("Поле с именем " + s + " должно присуствовать среди полей курсора"))    							 // если поле сортировки не присутствует среди полей курсора, то добавляем его
	    		);
	    		
	   		// -------------- получаем список полей сортировки и формируем компраратор для упорядочивания записей курсора -----------------------
   			Comparator<Object> comp1 =																						  // объявляем компраратор для упорядочивания записей курсора
   			Stream.of( orderByFields.split(",") ).map( s->s.trim() )  														  // отрезаем пробелы справа и слева от каждого поля сортировки
	   			.map( s-> ((s.split(" ").length == 2 && s.split(" ")[1].trim().compareToIgnoreCase("desc")==0)?  			  // выясняем указана ли опция desc у поля
	   						((Comparator<Object>)(r1, r2) ->  ((Record)r1).compareTo( s.split(" ")[0].trim(), (Record)r2 )).reversed() :  // если опция desc у поля указана 
	   						((Comparator<Object>)(r1, r2) ->  ((Record)r1).compareTo( s.split(" ")[0].trim(), (Record)r2 ))))             // если опция desc у поля не указана 
	   			.reduce( (a0,a1)-> a0=a0.thenComparing( a1 )).orElse(null);			// мы получили поток компараторов типа <сравнить запись по значению в полю>, компараторов столько сколько полецй в order by. Теперь объединяем их через thenComparing, используя инструкцию reduce  
	   			this.cursorStream = this.cursorStream.sorted( comp1);               // добавляем полученный компаратор в cursorStream как инструкцию
	   		}
			cursor = this.cursorStream.collect( Collectors.toList());                  // выполняем cursorStream для получения курсора
			this.setSelectStep( SelectStep.GetCursorEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this.cursor;
}
	    
/* ------------------ окончательное оформление Stream tableStream ------------------------------------------
* ---------- у нас имеется Stream, который возвращает Record со списком полей коллекции, указанной во from
* ---------- а нам теперь надо перейти к Record со списком полей, указанных Select
* ---------- также здесь применяем к Stream опцию distinct, если она была затребована
* ---------- также здесь формируем шапку (поля) курсора
*/
public OneTableWhere formCorsorStream(  ){
  	try {
	    if	( !cursorStreamFormed ) {	
    		cursorStream = cursorStream.map( r-> RecordFactory( r ));   // переход от записи коллекции к записи курсора
			if ( isDistinct ) cursorStream = cursorStream.distinct();   	  // применяем к Stream спецификацию distinct, если она была затребована
			setSelectFieldsAliases( ); 										  // формируем поля курсора
			cursorStreamFormed = true;
	    }
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return this;
}


/** создаем record с полями, указанными в Select на основе record коллекции, указанной во from */
public Record RecordFactory(Record r ) {
	Record rec = null; 
   	try {
		int fieldCount = selectFieldsAliases.fields.size();  						// количество полей в курсоре
		Object[] fieldsValues = new Object[fieldCount] ;    						// массив, в которолм будут храниться значения полей нового record
			
		Stream.iterate(0, i->i+1).limit(fieldCount) 								// с каждым полем, указанным в Select
			.forEach( i -> {	
				Field f = selectFieldsAliases.fields.get( i );     					// получаем поле
				// получаем значение поля, в зависимости от типа поля - простое или калькулируемое 
				FieldU fu = lambda.get( f );
				switch ( fu.fieldType ) {
				case Simple: 														// простое поле
				case Calc:                  										// калькулируемое поле
					fieldsValues[i] =  fu.lambda.apply( r );                        // рассчитываем значение поля
					break;
				}
			});
			rec = new Record( selectFieldsAliases, fieldsValues);					// формируем запись курсора 
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
	return rec;
}

//-------------- выводим на печать то, что вернул запрос -------------------    
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
//			.collect( StringBuilder::new,					   // сюда будем собирать линию разделитель	
//					  ( r, w )-> { char[] arr = new char[w]; Arrays.fill( arr , '-'); if ( r.length()==0) r.append(arr); else r.append("-+-").append(arr); },        // получам строку заполненную символов '-' и длинной равной ширигне колонки 
//					  ( r1, r2 ) -> {}//r1.append(r2).append("-+-")           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
//			).toString() + 
			"-+";                                          // завершаем разделительную линию

	System.out.println( "\nВ результате выполнения запроса <"+ this.SQLname +">\n" + this.SQLtext + "\nполучено:");
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

private static class FieldU<T> {
	Field f;
	FieldType fieldType;
	Function<? extends Record, T> lambda;  // лямбда, которая должна возвращать значение поля. Record - вход, ? super T - выход	
	private FieldU( Field f, FieldType fieldType, Function<? extends Record, T> lambda, Function<List<? extends Record>, T> lambdaAgregat ){
    	this.f = f;
    	this.fieldType = fieldType;
    	switch ( fieldType ) {
    	case Simple:
    	case Calc:
    		this.lambda = lambda;  // лямбда, которая должна возвращать значение поля. Record - вход, ? super T - выход
    		break;
    	}	
	}
	
	@Override
	public String toString(){
		return "поле " + f.fieldName + " типа " + fieldType.toString();
	}
}

// -------------  тип расчета значения поля -------------------   
enum FieldType { Simple, Calc }


	    
	    // -------------  этап в реализации select from -------------------   
	    enum  SelectStep{ SelectBegin, SelectEnd, FromBegin, FromEnd, WhereBegin, WhereEnd, 
	    	              GroupByBegin, GroupByEnd, HavingBegin, HavingEnd, 
	    	              UnionBegin, UnionEnd, MinusBegin, MinusEnd, IntersectionBegin, IntersectionEnd,
	    	              OrderByBegin, OrderByEnd,
	    	              GetCursorBegin, GetCursorEnd }
	    
	    // утилита по выводу данных на консоль на основе System.out.println  
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
	    public OneTableWhere setSQLName( String SQLname ){
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



