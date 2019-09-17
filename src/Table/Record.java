package Table;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Record {
	public TableFields recordFields;        // шапка таблицы - список полей
	public Object[] fieldsValues;          // свойства или реальные значения полей в записи


	public  Record( TableFields recordFields, Object... o ) {      // создаем record на основе ссылки на шапку полей и массива значений полей
            	// ------------ проверки параметров ---------------------

                if ( o == null || recordFields == null || recordFields.fields == null ) 
			 throw new IllegalArgumentException("значение параметров при создании record не могут быть null:\n o=" + o + "; recordFields=" + recordFields + "; recordFields.fields=" + recordFields.fields );
		if ( o.length == 0 || recordFields.fields.size() != o.length  ) 
			throw new IllegalArgumentException("размерности параметров должны быть одинаковыми и не равными 0:\n recordFields.fields.size()=" + recordFields.fields.size() + "; o.length=" + o.length ); 

		this.fieldsValues = o;                                    // фиксируем массив значений полей 
		this.recordFields = recordFields;			  // фиксируем ссылку на шапку полей 	
		}
	
	//получаем значение поля в записи по имени поля 
	public Object val( String fieldName ){
		return this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];   // ищем индекс поля и получаем значение в массиве значений
		};
		
        
	@Override
	public String toString() {                                                      // строковое представление записи в виде <имя поля><=><значение поля> 
		return Stream.iterate(0, i->i+1).limit(this.fieldsValues.length).
				map(i-> recordFields.fields.get(i).fieldName + "=" + ((fieldsValues[i]==null)?"null":fieldsValues[i].toString())).collect(Collectors.joining("; "));
	}
	

	
	// получаем значение поля, приведенное к типу String 
	public String Field( String fieldName ){
		Object o = this.fieldsValues[ this.recordFields.getFieldIndexByName(fieldName) ];		// получаем значение поля
		if (o instanceof String)  return (String)(o) ;                                                 // получаем значения типа String поля, если поле String
		throw new IllegalArgumentException("Не предусмотрено использование полей класса " + o.getClass().getName());
	}

        
        
        
        
	
}


