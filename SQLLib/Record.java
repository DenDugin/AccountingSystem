package SQLLib;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Record implements Comparable {
	public TableFields recordFields; // шапка таблицы - список полей
	public Object[] fieldsValues;          // реальные значения полей в записи


	public  Record( TableFields recordFields, Object... o ) {      // создаем record на основе ссылки на шапку полей и массива значений полей
		// ------------ проверки параметров ---------------------
		if ( o == null || recordFields == null || recordFields.fields == null ) 
			 throw new IllegalArgumentException("значение параметров при создании record не могут быть null:\n o=" + o + "; recordFields=" + recordFields + "; recordFields.fields=" + recordFields.fields );
		if ( o.length == 0 || recordFields.fields.size() != o.length  ) 
			throw new IllegalArgumentException("размерности параметров должны быть одинаковыми и не равными 0:\n recordFields.fields.size()=" + recordFields.fields.size() + "; o.length=" + o.length ); 

		this.fieldsValues = o;                                    // фиксируем массив значений полей 
		this.recordFields = recordFields;						  // фиксируем ссылку на шапку полей 	
		}
	
	/** получаем значение поля в записи по имени поля */
	public Object val( String fieldName ){
		return this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];   // ищем индекс поля и получаем значение в массиве значений
		};
		
	@Override
	public String toString() {                                                          // строковое представление записи в виде <имя поля><=><значение поля> 
		return Stream.iterate(0, i->i+1).limit(this.fieldsValues.length).
				map(i-> recordFields.fields.get(i).fieldName + "=" + ((fieldsValues[i]==null)?"null":fieldsValues[i].toString())).collect(Collectors.joining("; "));
	}
	
	@Override
	// сравниваем по значениям все поля записи (Record)obj со значениями полей записи this 
    public boolean equals(Object obj) {
	   if (!(obj instanceof Record)) throw new IllegalArgumentException("Не предусмотрено сравнените объекта класса Record и класса " + obj.getClass().getName());	
       return Stream.iterate(0, i->i+1).limit(this.recordFields.fields.size())  //перебираем все поля записи 
     		  .allMatch( i -> {													// allMatch - значит для всех полей записей значения должны совпасть и тогда только будет true
      			Object oThis = this.fieldsValues[ i ];                          // получаем значение поля в одной записи                          
      			Object oAnother = ((Record)obj).fieldsValues[ i ];              // получаем значение поля в другой записи
        		if (oThis instanceof Long)    return ((Long)oThis).equals(oAnother);   		// сравниеваем значения если поле типа long
      			if (oThis instanceof Integer) return ((Integer)oThis).equals( oAnother);	// сравниеваем значения если поле типа Integer
      			if (oThis instanceof String)  return ((String)oThis).equals(oAnother);		// сравниеваем значения если поле типа String
      			throw new IllegalArgumentException("Не предусмотрено использование полей класса " + oThis.getClass().getName());
      		  });
	}

	@Override // получаем hashCode на основе значений всех полей записи
	public int hashCode() {
		return Stream.iterate(0, i->i+1).limit(this.recordFields.fields.size())     // для каждого поля записи
		.map( i-> this.fieldHashCode( this.recordFields.fields.get(i).fieldName ) ) // получаем hash код значения поля
		.reduce((f1, f2) -> f1 + f2).orElse(0);		                                // суммируем hash значения полей записей   
	}

	/** получаем hash значения одного поля (fieldName) записи */
	public Integer fieldHashCode( String fieldName ){        
		Object o = this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];      // получаем значение поля
		if (o instanceof Long)  return ((Long)(o)).intValue() ;									// получаем hashCode, если поле Long 														
		if (o instanceof Integer)  return (Integer)(o) ;										// получаем hashCode, если поле Integer
		if (o instanceof String)  return ((String)o).hashCode();								// // получаем hashCode, если поле String
		throw new IllegalArgumentException("Не предусмотрено использование полей класса " + o.getClass().getName());
	}
	
	
	/** получаем значение поля, приведенное к типу Integer */
	public Integer asInt( String fieldName ){
		Object o = this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];     	// получаем значение поля
		if ( o instanceof Long)  return ((Long)o).intValue();									// получаем значения типа Integer поля, если поле Long
		if (o instanceof Integer)  return (Integer)(o) ;										// получаем значения типа Integer поля, если поле Integer
		if (o instanceof String)  return Integer.valueOf(((String)o));							// получаем значения типа Integer поля, если поле String
		throw new IllegalArgumentException("Не предусмотрено использование полей класса " + o.getClass().getName());
	}

	
	/** получаем значение поля, приведенное к типу String */
	public String asStr( String fieldName ){
		Object o = this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];		// получаем значение поля
		if (o instanceof Long)  return String.valueOf((Long)(o));								// получаем значения типа String поля, если поле Long
		if (o instanceof Integer)  return String.valueOf((Integer)(o));							// получаем значения типа String поля, если поле Integer
		if (o instanceof String)  return (String)(o) ;											// получаем значения типа String поля, если поле String
		throw new IllegalArgumentException("Не предусмотрено использование полей класса " + o.getClass().getName());
	}

	/** получаем значение поля, приведенное к типу Long */
	public Long asLong( String fieldName ){
		Object o = this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];		// получаем значение поля
		if (o instanceof Long)  return (Long)(o);												// получаем значения типа Long поля, если поле Long
		if (o instanceof Integer)  return Long.valueOf((Integer)(o));							// получаем значения типа Long поля, если поле Integer
		if (o instanceof String)  return Long.valueOf((String)(o)) ;							// получаем значения типа Long поля, если поле String
		throw new IllegalArgumentException("Не предусмотрено использование полей класса " + o.getClass().getName());
	}
	

	/** сравнение значения одного поля в двух объектах типа Record */  
	public int compareTo(String fieldName, Record r){
		Object value = val(fieldName);															// получаем значения поля
		if (value instanceof Integer) return ((Integer) value).compareTo( r.asInt(fieldName));	// сравниваем значения поля в двух записях, если поле типа Integer
		if (value instanceof Long)  return ((Long)value).compareTo( r.asLong(fieldName));		// сравниваем значения поля в двух записях, если поле типа Long
		if (value instanceof String)  return ((String)value).compareTo( r.asStr(fieldName));	// сравниваем значения поля в двух записях, если поле типа String
	    return 100;
	}

	@Override  /** сравнение двух записей по значениям всех полей */
	public int compareTo( Object o ){
 	   if (!(o instanceof Record)) throw new IllegalArgumentException("Не предусмотрено сравнение объекта класса Record и класса " + o.getClass().getName());	
		Record r = (Record) o;
		int b =
		Stream.iterate(0, i->i+1).limit(this.recordFields.fields.size())  // для i=0..[количество полей-1]
				.map( i->this.recordFields.fields.get(i))                 // получаем поле по его индексу
				.map(f-> this.compareTo( f.fieldName, r))                 // сравниваем значения в поле в сравниваемых record
				.filter( i -> i != 0 )                                    // фильтруем все поля, в которых одинаковые значения
				.findFirst().orElse(0);                                   // если хоть одно поле неотфильтровалось, то получим 1 или -1, если ни одного поля не прошло фильтр, то получим 0 
		return b;
	}

	
}


