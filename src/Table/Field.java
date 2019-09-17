package Table;


public class  Field<T> {
	String fieldName;  // имя поля
	Class<T> cls;      // для данного проекта класс поля типа String

	public Field(String fieldName, Class<T> cls ){
		if ( cls == null || fieldName == null )                // имя поля и класс должны быть определены
			 throw new IllegalArgumentException("При создании поля использованы null значения параметров cls=" + cls + ";  fieldName=" + fieldName );

        if ( !cls.equals(String.class)) 
            throw new IllegalArgumentException("в записи record предусмотрена возможность использования только полей классов String, а " + cls.getName() + "  " + cls.toGenericString() +  " не предусмотрено (поле " + fieldName + ")" );

		this.cls = cls;										// фиксируем класс поля
		this.fieldName = fieldName;								// фиксируем имя поля
	}
	
	@Override
	public String toString(){                                  // определяем текстовое представление поля  
	  	return "поле" + " " + fieldName + " класса " + ((cls==null)? "null" : cls.getName()); 
	}
}

