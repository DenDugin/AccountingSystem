package main;


import Table.Record;
import Table.TableFields;


/* 
 * Экземпляры данного класса наполняют коллекцию животных
 */

public class Animals extends Record {


        public Animals (TableFields masterFields,  Object... Properts  )
        {   

            super (masterFields, Properts);
            //this.Properts = Properts;
        }


}
