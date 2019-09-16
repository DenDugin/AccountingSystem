package main;

import java.util.function.Consumer;
import java.util.function.Supplier;

import SQLLib.Record;
import SQLLib.TableFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* для демонстрации примеров используется коллекция. Её элементами являются объекты этого класса. 
 * Аналог Entity в HIBERNATE  
 */

public class Provider extends Record{

	
	public Provider( TableFields masterFields, Integer provider_id, String provider_name, Integer provider_rating, String provider_city){
		super (masterFields, provider_id, provider_name, provider_rating, provider_city );
		//---- начало ------------  экпериментальная часть функционала --------------
		this.provider_id = provider_id;
		this.provider_name = provider_name;
		this.provider_rating = provider_rating;
		this.provider_city = provider_city;
		//---- окончание ------------  экпериментальная часть функционала --------------
	}
        
        
        public Provider (TableFields masterFields, List<String> Properts )
        {
            super (masterFields, Arrays.asList(Properts));
            this.Properts = Properts;

        }
//        

	// ---- начало ------------ экпериментальная часть функционала --------------
	// --- эти четыре функции необходимы для демонстрации работы select() с указанием полей, как указателей на методы --- 
	public static Integer getProvider_id(Provider p) {return ( p==null)? 0 : p.provider_id;};
	public static String getProvider_name( Provider m ){return  ((m==null)? "" :  ((Provider)m).provider_name);};
	public static Integer getProvider_rating( Provider m ){return ((m==null)? 0 : ((Provider)m).provider_rating);};
	public static String getProvider_city( Provider m ){return ((m==null)? "" :   ((Provider)m).provider_city);};

	// --- определяем имена и типы полей -----------
	public Integer provider_id;
	public String provider_name;
	public Integer provider_rating;
	public String provider_city;
        
        public List<String> Properts; 
        
        
	// ---- окончание ------------ экпериментальная часть функционала --------------

}
