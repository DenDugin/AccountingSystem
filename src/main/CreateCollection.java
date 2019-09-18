package main;

import java.util.ArrayList;
import java.util.List;
import Table.Field;
import Table.TableFields;
import java.io.IOException;
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import static Table.TableOperation.*;

/* 
 * класс организует наполнение данными коллекции Animals.
 */


public class CreateCollection {
    
    
	public static List<Animals> FillList() {
            
            
		List<Animals> ListAnimals = new ArrayList<>();    // инициализируем коллекцию, в которой будут лежать записи типа  Animals
                

                TableFields AnimalsFields = new TableFields ( ParseProperties() );  // создаем шапку коллекции (аналог названия полей таблицы) 

             
                ParseInput(AnimalsFields, ListAnimals); // добавдяем записи в таблицу
  
                
                return ListAnimals;    
	}
        
        // парсим названия полей(свойств животных) и получаем List полей шапки 
        private static List<Field> ParseProperties() 
        {
         List<Field> fields = null;   
      try {
            
            fields = new ArrayList<Field>();
          
            // Создается построитель документа
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            // Создается дерево DOM документа из файла
            Document document = documentBuilder.parse("Properties.xml");
 
            // Получаем корневой элемент
            Node root = document.getDocumentElement();
            
            
            // Просматриваем все подэлементы корневого
            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);

                // Если нода не текст, то это книга - заходим внутрь
                if (book.getNodeType() != Node.TEXT_NODE) {
                    NodeList bookProps = book.getChildNodes();
                    
                  if ( bookProps.item(0) == null ) throw new ParserConfigurationException("В Properties.xml найден пустой тэг"); 

                    fields.add(new Field<String>( bookProps.item(0).getTextContent(), String.class));

                }
            }
        
 
        } catch (ParserConfigurationException | SAXException | IllegalArgumentException | IOException ex) {
               
                ex.printStackTrace(System.out);            
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
                System.exit(1);
        }
       
      
         return fields;
            
        }
      
         // парсим input.xml и заполняем коллекцию Animals
         private static void ParseInput(TableFields AnimalsFields, List<Animals> m)
        {

         List<String> Properts;
         
      try {

            // Создается построитель документа
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            // Создается дерево DOM документа из файла
            Document document = documentBuilder.parse("Input.xml");
 
            // Получаем корневой элемент
            Node root = document.getDocumentElement();

            // Просматриваем все подэлементы корневого 
            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);
                // Если нода не текст, то это книга - заходим внутрь
                if (book.getNodeType() != Node.TEXT_NODE) {
                    int k = 0;
                    Properts  = new ArrayList<String>();
                    NodeList bookProps = book.getChildNodes();
                    for(int j = 0; j < bookProps.getLength(); j++) {
                        Node bookProp = bookProps.item(j);
                        // Если нода не текст, то это один из параметров книги
                        if (bookProp.getNodeType() != Node.TEXT_NODE) {
                            
                            if ( bookProp.getChildNodes().item(0) != null )
                            {
                                 // проверка соответствия полей  c properties.xml
                               if ( bookProp.getNodeName().equals(AnimalsFields.getFieldName(k)) == false )
                                   throw new ParserConfigurationException("Не соответствие названия полей в input.xml и properties.xml");
                  
                            Properts.add(bookProp.getChildNodes().item(0).getTextContent());                                
                            //System.out.println("This input : " + bookProp.getChildNodes().item(0).getTextContent()); 
                            k++;
                            }
                            else  Properts.add("NULL"); 

                        }
                    }

                    m.add( new Animals(AnimalsFields, Properts.toArray()));

                }
            }

        } catch (ParserConfigurationException | SAXException | IllegalArgumentException | IOException ex) {                
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
                System.exit(1);
        }

        }
      
        
         
         
    public static List<String> GetRules()                 
    {
           
      List<String> Rules = null; 
        
      try {
            Rules = new ArrayList<String>();
            
            // Создается построитель документа
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            // Создается дерево DOM документа из файла
            Document document = documentBuilder.parse("Rules.xml");
 
            // Получаем корневой элемент
            Node root = document.getDocumentElement();

            // Просматриваем все подэлементы корневого
            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);
                // Если нода не текст, то это книга - заходим внутрь
                if (book.getNodeType() != Node.TEXT_NODE) {
                    
                    NodeList bookProps = book.getChildNodes();
                    for(int j = 0; j < bookProps.getLength(); j++) {
                        Node bookProp = bookProps.item(j);
        
                        if (bookProp.getNodeType() == Node.CDATA_SECTION_NODE) {                            
                            if (bookProp.getTextContent().length() != 0)                       
                                    Rules.add(bookProp.getTextContent());                              
                        }     
                    }
                }
            }

        } catch (ParserConfigurationException | SAXException | IllegalArgumentException | IOException ex) {
                ex.printStackTrace(System.out);                
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
                System.exit(1);
        }
      
        return Rules;
      
        }        
             

}
