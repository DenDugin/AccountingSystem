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

public class Animals extends Record {


        public Animals (TableFields masterFields,  Object... Properts  )
        {   

            super (masterFields, Properts);
            //this.Properts = Properts;

        }


}
