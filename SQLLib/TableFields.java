package SQLLib;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * Объект данного класса содержит список полей (List<Field>) записи коллекции, 
 * или возвращаемого курсора (который также является коллекцией) 
 * и соответствует шапке таблицы в SQL
 */
public class TableFields {                                                  
	public List<Field> fields; 												// контейнер для хранения полей шапки таблицы

	protected TableFields( Field... fields){								// конструктор на основе массива полей
		this.fields = Arrays.stream(fields).collect( Collectors.toList());  // фиксируем поля шапки
	}

	public TableFields(List<Field> fields){                                 // конструктор на основе списка List<Field> полей      
		if ( fields == null || fields.size() == 0 ) throw new IllegalArgumentException("В шапке должно быть определено хотя бы одно поле!!!" ); 
		this.fields = fields;												// фиксируем поля шапки
	}

	
	/** находит поле шапки по имени поля */
	public Field getField( String fieldName  ){
		return fields.stream().filter( f-> f.fieldName.compareToIgnoreCase(fieldName) == 0 ).findFirst()   // получаем Stream на основе всех полей шапки и отфильровываем все с несовпадающими названиями
				.orElseThrow(()->new IllegalArgumentException("поле " + fieldName + " отсутствует среди полей:\n" + toString()  ));  // если через фильтр не прошло ни одного пля, значит искомого поля нет. Поднимаем ошибку.
	}
	
	/** находит имя поля шапки по порядковому номеру (индексу)*/
	public String getFieldName( Integer iCol ){
		if ( iCol >= fields.size()) throw new IllegalArgumentException("затребовано поле с порядковым номером " + iCol + ", а всего полей " + fields.size() + ". Список полей :\n" + toString()  );
		return fields.get( iCol ).fieldName;
	}

	/** находит индекс поля по имени поля*/
	public Integer getFieldIndexByName( String fieldName ){    
		return Stream.iterate(0, i->i+1).limit(this.fields.size())      					// получаем Stream из чисел 0...<кол-во полей-1>
				.filter(i-> fields.get(i).fieldName.compareToIgnoreCase(fieldName) == 0 )   // если поле с индексом i отличается от искомого имени поля, то отфильтровываем его 
				.findFirst()																// если нашли хоть одно прекращаем поиск
				.orElseThrow(()->new IllegalArgumentException("поле " + fieldName + " отсутствует среди полей:\n" + toString()  ));  // если не нашли ни одного, то генерим ошибку
	}

	/** находит класс значения поля по имени поля*/
	public Class getCls(  String fieldName ){
		return getField( fieldName ).cls;               // получаем класс значений поля
	}
	
	/** возвращает строковое представление шапки */
	@Override
	public String toString(){ 
	  	return fields.stream().map( f->f.toString()).collect(Collectors.joining("\n"));
	}
	
}
