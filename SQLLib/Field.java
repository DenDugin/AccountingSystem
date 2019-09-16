package SQLLib;

import java.util.List;
import java.util.function.Function;

public class  Field<T> {
	String fieldName;  // имя поля
	Class<T> cls;      // класс поля String, Integer, Long  

	public Field(String fieldName, Class<T> cls ){
		if ( cls == null || fieldName == null )                // имя поля и класс должны быть определены
			 throw new IllegalArgumentException("При создании поля использованы null значения параметров cls=" + cls + ";  fieldName=" + fieldName );
		if ( !cls.equals( Long.class ) && !cls.equals(Integer.class) && !cls.equals(String.class)   // класс значений в классе должны быть String, Integer или Long
			 && !cls.equals( long.class ) && !cls.equals(int.class)  	) 
			 throw new IllegalArgumentException("в записи record предусмотрена возможность использования только полей классов Integer, Long, String, а " + cls.getName() + "  " + cls.toGenericString() +  " не предусмотрено (поле " + fieldName + ")" );

		this.cls = cls;											// фиксируем класс поля
		this.fieldName = fieldName;								// фиксируем имя поля
	}
	
	@Override
	public String toString(){                                  // определяем тестовое представление поля  
	  	return "поле" + " " + fieldName + " класса " + ((cls==null)? "null" : cls.getName()); 
	}
}

