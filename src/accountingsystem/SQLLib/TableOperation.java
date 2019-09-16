package SQLLib;

//import static SQLLib.OneTableGroupBy.count;
import static SQLLib.TableOperation.select;
//import static SQLLib.OneTableGroupBy.union;

import java.awt.datatransfer.StringSelection;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
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

import main.CreateCollection;
import main.Animals;

import java.util.stream.StreamSupport;
import org.mvel2.MVEL;

/**
 * Данный класс реализует конструкцию 
 * select простые поля, *, калькулируемые поля, агрегируемые поля  
 * from [одна коллекция-таблица]
 * where [predicate]
 * group by
 * having
 * order by
 * union|minus|intersection
 * многоэтажный select 
 */

public class TableOperation {
	//public boolean trace = false;                   							// надо ли выводить сообщения о ходе выполнения запроса и подробную информацию о стеке ошибки
	public String QueryText = "";                    								// текстовое представление SQL запроса
	//public String SQLname = "";                                                 // имя запроса, необходимо только для отладочной информации
	public Stream<? extends Record> cursorStream;   							// стрим, выполнение которого вернет курсор
	//public Stream<Map.Entry<Record,List<Record>>> groupByStream = null;   		// промежуточный stream для group by	
	//public TableFields agregateTF;												// шапка курсора (поля курсора) для group by
	public List<? extends Record> table = new ArrayList<>(); 		  			// копия исходной коллекции-запроcа используемой в запросе (во from)
	public List<Record> cursor;           										// коллекция строк курсора
	//public TableFields selectFieldsAliases  = new TableFields();     			// поля коллекций-запросов полученные в курсоре (фактически их алиасы!), представлены именем и лямбдой для получения значения на основе записи (Record) получаемого курсора
	//public Map<Field, FieldU> lambda = new HashMap<>();  						// карта поле-лямбда, лямбда должна возвращать значение поля. Record - вход, ? super T - выход	
	public Object[] fields;          											// этот массив нам нужем для начальной фиксации набора полей, которые указаны при вызове from 
	//public boolean isDistinct = false;  										// надо ли удалять из курсора дубли строк, этот параметр устанавливается в начале конструкции select, но действие применяется в самом конце
   // public String orderByFields;                                                // поля сортировки
   // public SelectStep selectStep;   											// шаг в реализации запроса select from        
   // public boolean cursorStreamFormed = false;  								// этот флаг обеспечивает выполнение функции formCursorStream не более одного раза
        public boolean textExceptionAlreadyAdded = false;                           // этот флаг обеспечивает добавление в сообщение, информации о SQL запросе не более одного раза
        
        private static String FileRezult;

        public static BufferedWriter writer;
        
        public long count;
       

  /** -------------- реализуем having -------------------------- */ 
//  public OneTableGroupBy having( Predicate<? super Map.Entry<Record,List<Record>>>  p ){
//	try {
//		this.setSelectStep( SelectStep.HavingBegin );                             // отмечаем шаг выполнения Select From
//		SQLtext += "\nhaving <предикат>";  										  // добавляем шаг having в текстовое представление запроса	
//  	
//		this.groupByStream = this.groupByStream.filter( p );   					  // учитываем предикат having	
//
//		this.setSelectStep( SelectStep.HavingEnd );                               // отмечаем шаг выполнения Select From
//	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//  	return this;
//  }
    
  /** -------------- функции sum для агрегирования ----------------------------- */
//  public static FieldU<Long> sum( String agrFieldName, String alias ){  											// агрегируемое поле и алиас агрегированного поля 
//		return new FieldU<Long>(new Field<Long>( alias, Long.class ), FieldType.Agregate, 							// создаем новое агрегируемое поле
//		   null, listR-> listR.stream().map( r-> ((Record)r).asInt( agrFieldName ) ).mapToLong(i->i).sum());  		// определяем лямбду для получения значения агрегируемого поля sum		
//  }

  /** -------------- функции count для агрегирования ----------------------------- */
//  public static FieldU<Long> count( String agrFieldName, String alias ){  											// агрегируемое поле и алиас агрегированного поля
//    	return new FieldU<Long>(new Field<Long>( alias, Long.class ), FieldType.Agregate,  							// создаем новое агрегируемое поле
//    			  null, listR-> listR.stream().filter( r->r.val(agrFieldName)!=null).count());     					// определяем лямбду для получения значения агрегируемого поля count (null значения не учитываем)
//  }
    
  /** -------------- функции max для агрегирования ----------------------------- */
//  public static  FieldU<Long> max(String agrFieldName, String alias){   											 // агрегируемое поле и алиас агрегированного поля
//    	return new FieldU<Long>(new Field<Long>( alias, Long.class ), FieldType.Agregate,   						 // создаем новое агрегируемое поле
// 			    null, listR-> listR.stream().map( r-> ((Record)r).asInt( agrFieldName ) ).mapToLong(i->i).max().getAsLong());    	// определяем лямбду для получения значения агрегируемого поля max
//  }
//  
//  /** -------------- функции min для агрегирования ----------------------------- */
//  public static FieldU<Long> min(String agrFieldName, String alias){    											 // агрегируемое поле и алиас агрегированного поля
//    	return new FieldU<Long>(new Field<Long>( alias, Long.class ), FieldType.Agregate,                            // создаем новое агрегируемое поле
//  			    null, listR-> listR.stream().map( r-> ((Record)r).asInt( agrFieldName ) ).mapToLong(i->i).min().getAsLong());    	// определяем лямбду для получения значения агрегируемого поля min
//  }


  /** -------------- реализуем groupBy ---------------*/ 
//  public OneTableGroupBy groupBy( String... fields ){    									// массив имен полей, используемых для группировки
//	  try {
//    	this.setSelectStep(  SelectStep.GroupByBegin );                            			// отмечаем шаг выполнения Select From
//    	SQLtext += "\ngroup by " + Stream.of( fields ).collect( Collectors.joining(", "));  // добавляем шаг group by в текстовое представление запроса
//
//    	agregateTF = new TableFields(                                                       // формируем щапку полей используемых для группировки 
//    			Stream.of( fields )    														// для всех имен полей, указанных в group by
//    			.map( fn-> new Field( fn, table.get(0).recordFields.getCls( fn ) ))  		// создаем новое поле 
//    			.collect(Collectors.toList())); 											// получаем список полей, указанных в group by
//    	
//    	this.groupByStream = this.cursorStream                         						// если мы используем group by, то нам нужен не Stream<Record>, а Stream<Map.Entry<Record,List<Record>>> 
//    			        .map( r->(Record)r )
//						.collect(Collectors.groupingBy(r-> new Record( agregateTF, Stream.of( fields ).map( fn-> r.val(fn)).toArray() )))   // в groupBy передаем record коллекции, указанной во from, а получаем record с полями, указанных в group by  
//						.entrySet().stream()												// получаем Stream элементов map
//						;
//      	this.setSelectStep( SelectStep.GroupByEnd );                             			// отмечаем шаг выполнения Select From
//	  } catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//	  return this;
//  }
    
/** -------------- реализуем orderby ------------------*/ 
//public OneTableGroupBy orderBy( String fields ){    										// список полей сортировки, перечисленных, через запятую
//     try {
//    	this.setSelectStep( SelectStep.OrderByBegin );                             			// отмечаем шаг выполнения Select From
//    	SQLtext += "\norder by " + fields;  												// добавляем шаг order by в текстовое представление запроса
//    	
//    	orderByFields = fields;                                                             // здесь только фиксируем список имен файлов сортировки   
//    
//		this.setSelectStep( SelectStep.OrderByEnd );                             			// отмечаем шаг выполнения Select From
//      } catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//      return this;
//}
    
/** -------------- реализуем minus -------------------------- */ 
//public static OneTableGroupBy minus( OneTableGroupBy otFrom, OneTableGroupBy otMinus  ){  // otFrom - select из которого вычитается otMinus    
//   	try {
//   		otFrom.setSelectStep( SelectStep.MinusBegin ); 			                            // отмечаем шаг выполнения Select From
//    	otFrom.formCorsorStream( null );													// применяем отложенные действия на otFrom
//    	otMinus.formCorsorStream( null );                                                   // применяем отложенные действия на otMinus
//    	
//    	List<Integer> minusList = otMinus.cursorStream.distinct().map( r->r.hashCode()).collect( Collectors.toList());  // реализуем Stream otMinus и получаем список List 
//    	otFrom.cursorStream = otFrom.cursorStream.distinct().filter( f-> !minusList.contains(f.hashCode()));  		   // отфильтровываем из otFrom записи, которые есть в otMinus
//    	
//    	
//    	otFrom.setSelectStep( SelectStep.MinusEnd );                             			// отмечаем шаг выполнения Select From
//	} catch ( Exception e ) { throw otFrom.new StreamSQLException( e.getMessage(), e.getStackTrace());}
//    return otFrom;
//}
  
/** -------------- реализуем intersection -------------------------- */ 
//public static OneTableGroupBy intersection( OneTableGroupBy otFrom, OneTableGroupBy otIntersection  ){
//   	try {
//   		otFrom.setSelectStep( SelectStep.IntersectionBegin );                             // отмечаем шаг выполнения Select From
//    	otFrom.formCorsorStream( null );
//    	otIntersection.formCorsorStream( null );
//    	List<Integer> intersectionList = otIntersection.cursorStream.distinct().map( r->r.hashCode()).collect( Collectors.toList());
//    	otFrom.cursorStream =
//    	otFrom
//    	.cursorStream
//    	.distinct()
//    	.filter( f-> intersectionList.contains(f.hashCode()))
//    	;
//    	otFrom.setSelectStep( SelectStep.IntersectionEnd );                             // отмечаем шаг выполнения Select From
//	} catch ( Exception e ) { throw otFrom.new StreamSQLException( e.getMessage(), e.getStackTrace());}
//   	return otFrom;
//}    

/** -------------- реализуем union -------------------------- */ 
//public static OneTableGroupBy union(OneTableGroupBy... ot  ){
//   	try {
//   		ot[0].setSelectStep( SelectStep.UnionBegin );                             // отмечаем шаг выполнения Select From
//    	ot[0].formCorsorStream( null );                                           // выполняем все отложенные действия на первом OneTableGroupBy   
//    	TableFields tf =  ot[0].selectFieldsAliases;							  // ссылка на поля первого курсора. Эти поля должны быть сформированы и в следующих курсорах. 	
//    	ot[0].cursorStream =													  // будем добавлять в cursorStream первого OneTableGroupBy	новые инструкции	
//    		Stream.of(ot)										  				  // получаем Stream bp переданных OneTableGroupBy		
//    			.map( o-> o.formCorsorStream( tf ).cursorStream )				  // получаем Stream из  cursorStream  				  
//    			.reduce((o0,o1) -> {											  // объединяем все cursorStream, используя Stream.concat(o0, o1)  	 
//    			return Stream.concat(o0, o1);
//    			})
//    			.orElse(null)
//    			.distinct()														  // фильтруем неуникальные запсии в объединенном курсоре
//    			;
//		ot[0].setSelectStep( SelectStep.UnionEnd );                             // отмечаем шаг выполнения Select From
//	} catch ( Exception e ) { throw ot[0].new StreamSQLException( e.getMessage(), e.getStackTrace());}
//   	return ot[0];
//}
//    
    
    
    
// -------------- where с предикатом -------------------------- 
public TableOperation Where( Predicate<Record> predicate  ){
   	try {
   	//this.setSelectStep(  SelectStep.WhereBegin ); 	                            	// отмечаем шаг выполнения Select From
    	// SQLtext += "\nwhere <предикат>";											// добавляем в текстовое представление запроса информацию о where и предикате
        
    	this.cursorStream = this.cursorStream.filter(predicate);  					// добавляем в Stream курсора предикат - реализуем where
    	
        Stream<? extends Record> CountStream = table.stream();
      
       //Stream<? extends Record> Str = this.cursorStream;
       
      //Supplier<Stream<String>> streamSupplier = () -> this.cursorStream.of(fields); 
     
       long count = CountStream.filter(predicate).count();
       
       
       //writer.append("Result count = " + count);
        System.out.println("Result count = " + count);
 
        // this.setSelectStep( SelectStep.WhereEnd );  
         
        // System.out.println("selectStep = " + this.selectStep.toString());
                
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this;
}

// lib MVEL
public TableOperation Where( String Rule ){
   	
    try {
        
        QueryText = Rule; 

        Serializable expr = MVEL.compileExpression(Rule);

        this.cursorStream = this.cursorStream.filter(e->MVEL.executeExpression(expr,e,boolean.class));
                                  		
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this;
}


public TableOperation WhereCount ( String Rule ) {

      try {
        
        QueryText = Rule; 

        Serializable expr = MVEL.compileExpression(Rule);
        
        count = this.cursorStream.filter(e->MVEL.executeExpression(expr,e,boolean.class)).count();
        
        writer.append("Result count = " + count);
        writer.newLine();
        writer.newLine();
        
        // ad in file count result
                                  		
   	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this;  
    
    
}    
// -------------- запускаем на выполнение stream, возвращающий курсор -------------------------- 
//public OneTableGroupBy distinct(  ){
//   	isDistinct = true;							// фиксируем необходимость фильтрации из курсора дублей записей. Реализуется в  formCorsorStream() 
//   	return this;
//}
//    
// -------------- создаем объект OneTableWhhere и указываем используемые поля из коллекций-таблиц -------------------    
public static TableOperation select(Object...  fields ) {  // список названий полей и алиасов курсора
   	TableOperation ot = new TableOperation();                     // создаем объект на основе которого функционирует SQL запрос
   	try {
    	
            
    	// ------- шаг первый - создали объект OneTableGroupBy для сопровождения запроса --------------
    	//ot.setSelectStep( SelectStep.SelectBegin );                             // отмечаем шаг выполнения Select From
//    	ot.SQLtext += "Select " + Stream.of( fields )                           // начинаем формирование текстового представления запроса 
//    			.map( f -> {if ( f instanceof String ) return (String)f;		// если поле указано как String
//    						else return ( ((FieldU)f).fieldType + " " + ((FieldU)f).f.fieldName );  // если поле указано как FieldU
//    						})
//    			.collect(Collectors.joining( ", "));    	                    // объединяем информацию о всех полях в одну строчку

    	// -------- шаг второй - зафиксировали поля переданные при вызове select. Это могут быть String и Field (калькулируемые и агрегируемые)
    	// -------- мы не можем преобразовать String в поля, так как нам не изветны их классы (Integer, Long, String), они будут известны, только 
    	// -------- когда определим коллекцию-таблицу. Мы это сделаем в методе from. Аналогично мы не можем пока заменить "*" на список полей и 
    	// -------- сделаем это в методе from.
    	//ot.fields = fields;                                                     // фиксируем указанные поля 
//    	ot.printLn("Зафиксированы следующие поля:\n	" +  						// формируем текстовое сообщение для тестировки
//    			Stream.of(fields).map( o-> { if (o instanceof String) return (String)o;                      // если поле указано как String
//    			   							 else	if (o instanceof FieldU) return ((FieldU)o).toString();   // если поле указано как FieldU
//    			   							 else return "неправильное поле"; })
//    			.collect(Collectors.joining("\n	")) );                          // объединяем информацию о всех полях в одну строчку
    	
    	//ot.setSelectStep( SelectStep.SelectEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw ot.new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return ot; 
}



public static void CreateFileRez()
{
 try {   
     
 FileRezult  =  System.getProperty("user.dir") + "/Rezult.txt";    
 
 FileWriter fw = new FileWriter(FileRezult, false);   
 
 fw.close();
 
 writer = new BufferedWriter(new FileWriter(FileRezult, true)); 
  
 } catch ( Exception e )
     
 {
  e.getMessage();
 }
 
}  


public static void CloseFile()
{
 try {
     writer.close();
     
 }  catch ( Exception e ) 
 {
 e.getMessage();
 }
 
}        
        
    

public static void AddToFile(String Data)
{
 try {
     
    writer.append(Data);
    writer.newLine();
     
 }  catch ( Exception e ) 
 {
 e.getMessage();
 }
 
}     



 public static TableOperation CreateTable()
{
 return new TableOperation();    
}




public TableOperation SelectFrom( List<? extends Record> from ) {

try {
   	
        if ( from.size() == 0 ) throw new IllegalArgumentException("Не предусмотрено использование пустых коллекций"); 
        
    	table = from;                                                           // фиксируем коллкцию
    	cursorStream = from.stream();	// Create Streams !!!			//  создание Stream на основе записей коллекции

	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this; 

    
}

    
    // -------------- определяем коллекцию-таблицу используемую в запросе и стрим, выполнение которого возвратит курсор -------------------
public TableOperation from( List<? extends Record> from ){
   	try {
   		//this.setSelectStep( SelectStep.FromBegin );                             // отмечаем шаг выполнения Select From
   		if ( from.size() == 0 ) throw new IllegalArgumentException("Не предусмотрено использование пустых коллекций"); 
//   		SQLtext += "\nfrom " + from.get(0).getClass().getCanonicalName();       // добавляем в тестовое представление запроса информацию о коллекции, указанной в select 
//   		printLn( "Во from используется коллекция записей "+from.get(0).getClass().getCanonicalName()+" с полями:\n	" +   // для трассировки выводим информацию о коллекции 
//   				from.get(0).recordFields.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );    // и ее полях
   		// ------ создаем шапку-заголовок коллекции-таблицы  
    	//TableFields tf = from.get(0).recordFields;

    	// ---- копируем данные из исходной коллекции, указанной в переменной from в ее копию this.table
    	// ---- некоторые действия могут изменить данные используемой коллекции, а select from не должен менять данные
    	// ---- данный Stream скопирует (в свое время) исхордную коллекцию в коллекцию table -----------
    	//from.stream().forEach( r->this.table.add( new Record( tf, r.fieldsValues )));
    	table = from;                                                           // фиксируем коллкцию
    	cursorStream = from.stream();	// Create Streams !!!										// оницируем создание Stream на основе записей коллекции
    	
		//this.setSelectStep( SelectStep.FromEnd );                             // отмечаем шаг выполнения Select From
	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
   	return this; 
}

//------ создаем шапку-заголовок курсора ------------------------
//public void setSelectFieldsAliases( TableFields anotherTF ) {
//	//------ создаем шапку-заголовок коллекции из from
//	TableFields tf = table.get(0).recordFields;
// 	
//	Stream.of(fields )  // перебираем поля, указанные при вызове select() 
//	.forEach( o-> {     // переходим от Object к FiledU или String и заполняем список полей курсора
//		if (o instanceof String) {
//			  String fs = (String)o;   // если в select обозначили поле через строку
//			  if ( fs.equals("*"))  {   // если звездочка, то размножаем строки и получаем список полей
//				 if ( groupByStream == null ) { // если не заявлен group by, то берем поля из коллекции во from   
//					 tf.fields.stream()    // для каждого поля коллекции
//					 .forEach( f0-> {
//                  			    Field f = new Field( f0.fieldName, f0.cls );
//			    	 			selectFieldsAliases.fields.add( f );    // добавляем поле в список курсора
//			    	 			lambda.put( f, new FieldU( f, FieldType.Simple, r-> f.cls.cast(((Record)r).val(f.fieldName)),  null ) );   // фиксируем лямбду для вычисления значения поля 
//			     				});
//				 }
//				 else {  					// если заявлен group by, то берем поля из agregateTF 
//					 agregateTF.fields.stream()    // для каждого поля агрегации 
//					 .forEach( f0-> {
//                  			    Field f = new Field( f0.fieldName, f0.cls );
//			    	 			selectFieldsAliases.fields.add( f );    // добавляем поле в список курсора
//			    	 			lambda.put( f, new FieldU( f, FieldType.Simple, r-> f.cls.cast(((Record)r).val(f.fieldName)), null ) ); // фиксируем лямбду для вычисления значения поля
//			     				});
//				 }
//			  }
//			  else {                   // если поле указано не как звездочка, а как имя поля и алиас
//				  //отделяем имя поля от алиаса
//				  String[] sArr;
//				  if ( fs.toUpperCase().contains( " AS ") ) sArr = fs.toUpperCase().split(" AS ");  // если в тексте строки есть " as "
//				  else sArr = fs.split(" ");														// если в тексте строки нет " as "		
//				  sArr[0] = sArr[0].trim();															// удаяляем в имени поля пробелы справа и слева
//				  if ( sArr.length == 2 ) sArr[1] = sArr[1].trim();									// удаяляем в алиасе поля пробелы справа и слева
//				  if ( sArr.length > 2 ) throw new IllegalArgumentException("Простое поле в select должно задаваться в формате ИмяПоля[ [as ИмяАлиаса]], а задано - " + fs );
//    			  else {
//    				  Field f = new Field( (sArr.length==1) ? sArr[0] : sArr[1], tf.getCls(  sArr[0]  ) );  				// создаем поле
//    				  selectFieldsAliases.fields.add( f );          														// добавляем поле в список полей курсора
//    				  lambda.put( f, new FieldU( f, FieldType.Simple, r-> f.cls.cast(((Record)r).val(sArr[0])), null ) );  	// добавляем мотод рассчета значения поля в список
//    			  }
//			  }
//		}
//		else if (o instanceof FieldU) {   						// если в select был пеоредан FieldU
//			selectFieldsAliases.fields.add( ((FieldU)o).f );  	// добавляем поле в список полей курсора
//			lambda.put( ((FieldU)o).f, (FieldU)o );				// фиксируем лямбда для вычисления поля
//		}
//});
// 	
//	// -------------- проверка: поля курсора должны иметь уникальные имена ------------------------------------------
//	String sDouble = selectFieldsAliases.fields.stream()     // получаем множество полей курсора
//		 .collect( Collectors.groupingBy( f -> new String(f.fieldName))) // группируем по именам полей
//		 .entrySet().stream()                 // получаем стрим сгруппированных полей
//		 .filter(e->e.getValue().size()>1)    // отфильтровываем поля, с уникальным именем
//		 .map( e->e.getKey())				  // переходим к именам полей	
//		 .collect( Collectors.joining("\n	"));  // объединяем задиблированные имена поелей в олдну строку
//	if ( sDouble.length() > 0 ) throw new IllegalArgumentException("В курсоре задублированы имена алиасов:\n	" + sDouble);   
//	
//	// -------------- для union необходимо, чтобы поля имели одинаковые (такие как у первого запроса) имена полей 
//	if ( anotherTF != null ) 
//		selectFieldsAliases.fields.stream().  
//		forEachOrdered( f-> f.fieldName =   // присваиваем имя поля из другой шапки 
//		   anotherTF.getFieldName( selectFieldsAliases.getFieldIndexByName(f.fieldName) )); 
// 		
//	printLn( "В курсоре на основе алиасов созданы следующие поля:	\n	" + selectFieldsAliases.fields.stream().map(f->f.toString()).collect(Collectors.joining("\n	")) );    
//}	

// -------------- запускаем на выполнение stream, и возвращаем курсор -------------------------- 
//public List<Record> getCursor(  ){
//   	try {
//   		this.setSelectStep( SelectStep.GetCursorBegin );                             // отмечаем шаг выполнения Select From
//    		
//    	formCorsorStream( null );  // окончательное оформление Stream tableStream 
//
//    	if ( orderByFields != null ) {  											 // если была задана сортировка
//    		// если была затребована сортировка и в списке полей полей, перечисленных в order by есть поле, которого нет в полях курсора, то генерим ошибку
//	    	Stream.of( orderByFields.split(",") )   								// -------------- получаем стрим <поле сортировки[ desc]>  -----------------------
//   				.map( s->s.trim().split(" ")[0].trim() )  							// отрезаем пробелы справа и слева от каждого поля сортировки и отрезаем desc
//   				.forEach( s->                										// с каждым полем сортировки
//   					selectFieldsAliases.fields.stream().filter( f-> (f.fieldName.compareToIgnoreCase( s )== 0))  // фильтр пропустит из полей курсора только поля, совпадающие с полем сортировки 
//   					.findFirst().orElseThrow(()->new IllegalArgumentException("Поле с именем " + s + " должно присуствовать среди полей курсора"))    							 // если поле сортировки не присутствует среди полей курсора, то добавляем его
//	    		);
//    	
//	   	// -------------- получаем список полей сортировки и формируем компаратор для упорядочивания записей курсора -----------------------
//   		Comparator<Object> comp1 =																						  // объявляем компраратор для упорядочивания записей курсора
//   		Stream.of( orderByFields.split(",") ).map( s->s.trim() )  														  // отрезаем пробелы справа и слева от каждого поля сортировки
//	   		.map( s-> ((s.split(" ").length == 2 && s.split(" ")[1].trim().compareToIgnoreCase("desc")==0)?  			  // выясняем указана ли опция desc у поля
//	   					((Comparator<Object>)(r1, r2) ->  ((Record)r1).compareTo( s.split(" ")[0].trim(), (Record)r2 )).reversed() :  // если опция desc у поля указана 
//	   					((Comparator<Object>)(r1, r2) ->  ((Record)r1).compareTo( s.split(" ")[0].trim(), (Record)r2 ))))             // если опция desc у поля не указана 
//	   		.reduce( (a0,a1)-> a0=a0.thenComparing( a1 )).orElse(null);			// мы получили поток компараторов типа <сравнить запись по значению в полю>, компараторов столько сколько полецй в order by. Теперь объединяем их через thenComparing, используя инструкцию reduce  
//	   		this.cursorStream = this.cursorStream.sorted( comp1);               // добавляем полученный компаратор в cursorStream как инструкцию
//	   	}
//		cursor = this.cursorStream.collect( Collectors.toList());                  // выполняем cursorStream для получения курсора
//		this.setSelectStep( SelectStep.GetCursorEnd );                             // отмечаем шаг выполнения Select From
//	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//   	return this.cursor;
//}
//    
    /* ------------------ окончательное оформление Stream tableStream ------------------------------------------
     * ---------- у нас имеется Stream, который возвращает Record со списком полей коллекции, указанной во from
     * ---------- а нам теперь надо перейти к Record со списком полей, указанных Select
     * ---------- при этом, если нет groupBy, то в Select поля из коллекции, указанной во from и калькулируемые
     * ---------- и для получения курсора используется Stream<? extends Record> tableStream,
     * ---------- а если есть group by, то в Select поля из groupBy и агрегируемые, и для получения курсора 
     * ---------- используется  Stream<Map.Entry<Record,List<Record>>> groupByStream.
     * ---------- также здесь применяем к Stream опцию distinct, если она была затребована
     * ---------- также здесь формируем шапку (поля) курсора
    */
//public OneTableGroupBy formCorsorStream( TableFields anotherTF ){
//   	try {
//    if	( !cursorStreamFormed ) {	
//    	if( this.groupByStream == null ) {   // если не была заявлена группировка groupBy
//    		cursorStream = cursorStream.map( r-> RecordFactory( r, null ));   // используем  Stream<? extends Record> tableStream
//    	} 
//    	else {                               // если была заявлена группировка groupBy
//    		cursorStream = groupByStream.map( e-> RecordFactory( e.getValue().get(0), e.getValue() ));  // используется  Stream<Map.Entry<Record,List<Record>>> groupByStream
//    	}
//		if ( isDistinct ) cursorStream = cursorStream.distinct();   //применяем к Stream опцию distinct, если она была затребована
//		setSelectFieldsAliases( anotherTF ); // формируем поля курсора
//		cursorStreamFormed = true;
//    }
//	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//   	return this;
//}


    /* ---------- создаем record с полями, указанными в Select на основе record коллекции, указанной во from  
     * ---------- при этом, если нет groupBy, то в Select поля из коллекции, указанной во from и калькулируемые поля
     * ---------- и для получения курсора используется Record,
     * ---------- а если есть group by, то в Select поля из groupBy и агрегируемые, и для получения курсора 
     * ---------- используется  Map.Entry<Record,List<Record>>, разбитый на Record и List<Record>
    */
//public Record RecordFactory(Record r, List<? extends Record> listR ) {
//	Record rec = null; 
//   	try {
//		int fieldCount = selectFieldsAliases.fields.size();   				// количество полей в Select
//		Object[] fieldsValues = new Object[fieldCount] ;    				// массив, в которолм будут храниться значения полей нового record
//		
//		Stream.iterate(0, i->i+1).limit(fieldCount) 						// с каждым полем, указанным в Select
//		.forEach( i -> {
//			Field f = selectFieldsAliases.fields.get( i );      			// ссылка на шапку курсора (поля курсора)
//			
//			// получаем значение поля, в зависимости от типа поля - простое, калькулируемое или агрегируемое
//			FieldU fu = lambda.get( f );
//			switch ( fu.fieldType ) {
//			case Simple:                                            		// простое поле
//			case Calc:                  									// калькулируемое поле
//				fieldsValues[i] =  fu.lambda.apply( r );					// выполняем лямбду для вычисления значения поля
//				break;
//			case Agregate:   												// агрегируемое поле	
//				fieldsValues[i] =  fu.lambdaAgregat.apply( listR );			// выполняем лямбду для вычисления значения поля
//				break;
//			}
//		});
//		rec = new Record( selectFieldsAliases, fieldsValues);		
//	} catch ( Exception e ) { throw new StreamSQLException( e.getMessage(), e.getStackTrace());}
//	return rec;
//}
    
	// -------------- выводим на печать то, что вернул запрос -------------------    
	public List<Record> printCursor( boolean prettyPrint ){
	     if ( prettyPrint ) {	// если запрошена приятная печать результата
	    	// --------------- выполняем Stream и получаем реальный список записей курсора ---------------- 
//	    	List<Record> l = null;//getCursor();    
//	    	
//	    	// --------------- инициируем массив значений ширины колонок значениями длинны имени поля ----------------    	
//	    	Integer[] maxSize =  this.selectFieldsAliases.fields.stream().map( f-> f.fieldName.length()).toArray(Integer[]::new);  // массив значений ширины колонок, заполняем изначально длинной имени поля
//	    	
//	    	// ------ корректируем ширину колонок таблицы, чтобы в ней поместилось значение поля любой записи курсора -------------- 
//	    	l.stream()   // для каждой строки курсора
//	    	.forEachOrdered( r-> Stream.iterate( 0, i->i+1 ).limit(maxSize.length)  //для каждого поля курсора 
//	    					 .forEachOrdered( i-> maxSize[i] = Math.max(maxSize[i], r.Field(r.recordFields.fields.get(i).fieldName).length()))  // если длинна String выражения значения поля больше, то увеличиваем ширину колонки 
//	  				    );
//
//	    	// собираем формат для вывода записи курсора, что-то вроде "| %-<ширина колонки1>.<ширина колонки1>s | %-<ширина колонки2>.<ширина колонки2>s |\n   ----------------------
//	    	String fieldsFormat = Stream.of( maxSize )  							// для каждого поля курсора
//		    		    .map( i->i.toString())  									// переводим размер ширины поля в String 
//		    			.reduce("| ", (s,s1)->s+"%-"+ s1+"."+ s1+"s | ")+ "\n"; 	// определяем правило собирания результирующей строки - флрмата вывода полей запсиси
//	    	
//	    	// собираем линию разделитель выводимую между записями курсора, что-то вроде +----+----------+--------+
//	    	String lineFormat =
//	    			"+-" +                                         // начинаем разделительную линию 
//	    			Stream.of( maxSize )                           // для каждого поля курсора берем вычисленную для него ширину
//	    			//.parallel()								   // раскомментарить для параллельного вычисления, чтобы получить ошибку <Эту операцию нельзя выполнять параллельно>	
//					.collect( Collector.of(						   // создаем собственный коллектор	
//	    					StringBuilder::new,					   // сюда будем собирать линию разделитель	
//	    					( r, w )-> { char[] arr = new char[w]; Arrays.fill( arr , '-'); if ( r.length()==0) r.append(arr); else r.append("-+-").append(arr); },        // получам строку заполненную символов '-' и длинной равной ширигне колонки 
//	    					//( r1, r2 ) -> r1.append(r2).append("-+-"),           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
//	    					( r1, r2 ) -> {throw new IllegalArgumentException("Эту операцию нельзя выполнять параллельно!");},           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
//	    					StringBuilder::toString                // заключительная операция - переводим StringBuilder в String
//	    			)) +
////	    			.collect( StringBuilder::new,					   // сюда будем собирать линию разделитель	
////	    					  ( r, w )-> { char[] arr = new char[w]; Arrays.fill( arr , '-'); if ( r.length()==0) r.append(arr); else r.append("-+-").append(arr); },        // получам строку заполненную символов '-' и длинной равной ширигне колонки 
////	    					  ( r1, r2 ) -> {}//r1.append(r2).append("-+-")           // здесь надо генерить ошибку, потому, что это нельзя выполнять в многопоточном режиме
////	    			).toString() + 
//	    			"-+";                                          // завершаем разделительную линию
//
//	    	System.out.println( "\nВ результате выполнения запроса \n" + this.SQLtext + "\nполучено:");
//	    	// --------------- собственно вывод содержимого таблицы на консоль -------------------------------
//		    System.out.println( lineFormat);      // выводим верхнюю линию таблицы
//		    System.out.printf( fieldsFormat, this.selectFieldsAliases.fields.stream().map( f-> f.fieldName ).toArray( String[]::new));  // выводим названия полей курсора
//		    System.out.println( lineFormat);      // выводим двойную разделительную линию, меду шапкой и телом таблицы
//		    System.out.println( lineFormat);
//		    l.stream().forEachOrdered( r->System.out.printf( fieldsFormat  + lineFormat + "\n", r.fieldsValues ));  // выводим значения строки куросора и разделительную линию
	      }
	      else   // если запрошена простая печать результата
	      {
	    	  // formCorsorStream( null );  														// окончательное оформление Stream tableStream 
	   		//  this.cursorStream.forEach(System.out::println);								// выводим записи на консоль
      
                 // setSelectFieldsAliases(null);
                        
                  // for (Field f : this.selectFieldsAliases.fields)
                  //    System.out.print(f.fieldName+"|");  

                  
                  
List<Record> l = this.cursorStream.collect( Collectors.toList());                  // выполняем cursorStream для получения курсора
if ( l.size()>0 )
{     
     System.out.println("Result count = " + l.size());
    System.out.println("______________________________");
      for (int i=0; i<l.get(0).recordFields.fields.size(); ++i )
    {          
        System.out.print(l.get(0).recordFields.getFieldName(i)+ " | ");  
  
    }
      System.out.println("");
      System.out.println("--------------------------------");
        

   for (int i=0; i<l.size(); ++i )
    {          
     for (int j=0;j<l.get(i).recordFields.fields.size(); ++j)   
        System.out.print(l.get(i).Field(table.get(i).recordFields.fields.get(j).fieldName)+" | ");  
        System.out.println("");
        
    }
System.out.println("--------------------------------");
               //   for (Record r : cursor)
               //       System.out.println(r.fieldsValues);  


}
else System.out.println("Result count = 0");

//                  System.out.println(this.selectFieldsAliases.fields.stream().map(f->f.fieldName));
                  
    // this.selectFieldsAliases.fields.stream().forEach( r->System.out.println( r.fieldName ));  // выводим значения строки куросора и разделительную линию
          
    //System.out.println(this.selectFieldsAliases.fields.stream().map( r->r.fieldName ).collect(Collectors.toList()));  // выводим значения строки куросора и разделительную линию
    
    
     //   this.cursorStream.forEach( r->System.out.println( r.fieldsValues ));
    
            
       // cursor = this.cursorStream.collect( Collectors.toList());                  // выполняем cursorStream для получения курсора
                
      //  for (Record r : this.cursor)
      //            System.out.println(r.fieldsValues.toString()); 
        
        
        
    
	      }
	   	return this.cursor;
	}    
    
        
        
        
  public void PrintRezult() {      
        
try {      
      
List<Record> l = this.cursorStream.collect( Collectors.toList());                  // выполняем cursorStream для получения курсора
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
     {System.out.print(l.get(i).Field(table.get(i).recordFields.fields.get(j).fieldName)+" | ");  
     writer.append(l.get(i).Field(table.get(i).recordFields.fields.get(j).fieldName)+" | ");}
        System.out.println(""); 
         writer.newLine();
    }
   
System.out.println("--------------------------------");
writer.append("--------------------------------\n\n\n");


               //   for (Record r : cursor)
               //       System.out.println(r.fieldsValues);  


}
else System.out.println("Result count = 0");

} catch ( Exception e ) 
{
    System.out.println(e.getMessage());   
}
     
}
        
        

    
//    private static class FieldU<T> {
//    	Field f;
//    	FieldType fieldType;
//    	Function<? extends Record, T> lambda;  // лямбда, которая должна возвращать значение поля. Record - вход, ? super T - выход	
//    	Function<List<? extends Record>, T> lambdaAgregat;  // лямбда, типа функциональный интерфейс, которая должна возвращать значение калькулируемого поля. Record - вход, ? super T - вывход
//    	private FieldU( Field f, FieldType fieldType, Function<? extends Record, T> lambda, Function<List<? extends Record>, T> lambdaAgregat ){
//        	this.f = f;
//        	this.fieldType = fieldType;
//        	switch ( fieldType ) {
//        	case Simple:														// простое поле
//			case Calc:                  										// калькулируемое поле
//        		this.lambda = lambda;  											// лямбда, которая должна возвращать значение поля. Record - вход, ? super T - выход
//        		break;
//        	case Agregate:														// агрегируемое поле
//        		this.lambdaAgregat = lambdaAgregat; 							// лямбда, типа функциональный интерфейс, которая должна возвращать значение калькулируемого поля. Record - вход, ? super T - вывход
//        		break;
//        	}	
//    	}
//    	
//    	@Override
//    	public String toString(){												// текстовое представление объекта FieldU
//    		return "поле " + f.fieldName + " типа " + fieldType.toString();
//    	}
//    	
//    }
    // -------------  тип расчета значения поля -------------------   
//    enum FieldType { Simple, Calc, Agregate, Analityc    }

    // утилита по выводу данных на консоль на основе System.out.println  
//    public void printLn( String s ) {
//    	if (trace) System.out.println( selectStep + "-->" + s);
//    }

    /** добавляемый текст к сообщению об ошибке */
    public String addTextException(){
      if ( textExceptionAlreadyAdded ) return "";
      textExceptionAlreadyAdded = true;
      return "\nПри обработке запроса : " + this.QueryText +     		  
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
//    public OneTableGroupBy setSQLName( String SQLname ){
//    	this.SQLname = SQLname;
//    	return this;
//    }
    
       
    /** при смене шага выполнения select .. from .. проводим проверки правильной последовательности шагов */     
//    public void setSelectStep( SelectStep ss ) {
//    	if ( this.selectStep != null ) {  // осуществляем проверки последовательности шагов формирования select from 
//     	   if (this.selectStep.name().endsWith("End") && ss.name().endsWith( "Begin" ) && (ss.ordinal() - this.selectStep.ordinal() == -1)) 
//               throw new IllegalArgumentException( "операция " + this.selectStep + " выполнена и ее нельзя начинать повторно. (вероятно " + 
//            		   this.selectStep.toString().substring(0, 1).toLowerCase() + this.selectStep.toString().substring(1, this.selectStep.toString().length() - 3) + " вызвали повторно)."  );
//     	   if (this.selectStep.ordinal() >= ss.ordinal() ) 
//     		   throw new IllegalArgumentException( "операция " + ss + " должна предшествовать операции " + this.selectStep + 
//     				   ". (вероятно " + ss.toString().substring(0, 1).toLowerCase() + ss.toString().substring(1, ss.toString().length() - 5) + 
//     				   " вызвали после " + this.selectStep.toString().substring(0, 1).toLowerCase() + this.selectStep.toString().substring(1, this.selectStep.toString().length() - 3) + ")." );
//     	   if (this.selectStep.name().endsWith("Begin") && ss.name().endsWith( "Begin" ) ) 
//               throw new IllegalArgumentException( "операция " + this.selectStep + " еще не закончилась. Нельзя начинать операцию " + ss );
//     	   if (this.selectStep.name().endsWith("End") && ss.name().endsWith( "End" ) ) 
//               throw new IllegalArgumentException( "операция " + ss + " заканчивается еще не начавшись" );
//    	} 	   
//       this.selectStep = ss;
//       printLn( "-----------------------------" + this.selectStep.toString() + "-----------------------------------");
//    }
    
}




