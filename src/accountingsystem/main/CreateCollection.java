package main;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import SQLLib.Field;
import SQLLib.TableFields;
import SQLLib.TableOperation;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import static SQLLib.TableOperation.*;

/* 
 * класс организует наполнение данными коллекции Animals.
 */


public class CreateCollection {
    
        // 2 do catch
    
	public static List<Animals> FillList() {
            
            
		List<Animals> ListAnimals = new ArrayList<>();    // инициализируем коллекцию, в которой будут лежать записи типа  Animals
                

                TableFields AnimalsFields = new TableFields ( ParseProperties() );  // создаем шапку коллекции (аналог названия полей таблицы) 

             
                ParseInput(AnimalsFields, ListAnimals); // добавдяем записи в таблицу
                
          
                //System.out.println("ListAnimals size " + ListAnimals.size());   
                
                return ListAnimals;    
	}
        
        // парсинг названия полей(свойств животных) и получаем List полей шапки 
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
            
            
            // Просматриваем все подэлементы корневого - т.е. книги
            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);
                // Если нода не текст, то это книга - заходим внутрь
                if (book.getNodeType() != Node.TEXT_NODE) {
                    NodeList bookProps = book.getChildNodes();
                    for(int j = 0; j < bookProps.getLength(); j++) {
                        Node bookProp = bookProps.item(j);
                        // Если нода не текст, то это один из параметров книги - печатаем
                        if (bookProp.getNodeType() != Node.TEXT_NODE) {
                            
                            if (  bookProp.getNodeName().equals("Title") )                                 
                            { //System.out.println("This properites : " + bookProp.getChildNodes().item(0).getTextContent()); 
                              fields.add(new Field<String>( bookProp.getChildNodes().item(0).getTextContent(), String.class));
                            }
                            
                            //System.out.println(bookProp.getNodeName() + ":" + bookProp.getChildNodes().item(0).getTextContent());
                        }
                    }
                    //System.out.println("===========>>>>");
                }
            }
        
 
        } catch (ParserConfigurationException ex) {
               
                ex.printStackTrace(System.out);            
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
 
        } catch (SAXException ex) {
            
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
                
            } catch (IllegalArgumentException ex) {
            
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();        
                
                
        } catch (IOException ex) {
            
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
        }
       
      
         return fields;
            
        }
      
         // парсим input.xml и заполняем коллекцию Animals
         private static void ParseInput(TableFields providerFields, List<Animals> m)
        {

         List<String> Properts;
         
      try {

            // Создается построитель документа
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            // Создается дерево DOM документа из файла
            Document document = documentBuilder.parse("Input.xml");
 
            // Получаем корневой элемент
            Node root = document.getDocumentElement();
            
        
            
            // Просматриваем все подэлементы корневого - т.е. книги
            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);
                // Если нода не текст, то это книга - заходим внутрь
                if (book.getNodeType() != Node.TEXT_NODE) {
                    Properts  = new ArrayList<String>();
                    NodeList bookProps = book.getChildNodes();
                    for(int j = 0; j < bookProps.getLength(); j++) {
                        Node bookProp = bookProps.item(j);
                        // Если нода не текст, то это один из параметров книги - печатаем
                        if (bookProp.getNodeType() != Node.TEXT_NODE) {
                            
                            if ( bookProp.getChildNodes().item(0) != null )
                            {
                            Properts.add(bookProp.getChildNodes().item(0).getTextContent());                            
                            //System.out.println("This input : " + bookProp.getChildNodes().item(0).getTextContent()); 
                            }
                            else  Properts.add("NULL"); 
                            
                            //System.out.println(bookProp.getNodeName() + ":" + bookProp.getChildNodes().item(0).getTextContent());
                        }
                    }
                    //System.out.println("Properts = " + Properts);
                    m.add( new Animals(providerFields, Properts.toArray()));
                    //System.out.println("===========>>>>");
                }
            }

        } catch (ParserConfigurationException ex) {                
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
                
        } catch (SAXException ex) {
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();     
         
           } catch (IllegalArgumentException ex) {
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();         
                
                
        } catch (IOException ex) {
                ex.printStackTrace(System.out);                
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
        }

        }
      
        
         
         
    public static List<String> GetRules()                 
    {
           
      List Rules = null; 
        
      try {
          
            Rules  = new ArrayList<String>();  
          
            // Создается построитель документа
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            // Создается дерево DOM документа из файла
            Document document = documentBuilder.parse("Rules.xml");
 
            // Получаем корневой элемент
            Node root = document.getDocumentElement();
            
        
            
            // Просматриваем все подэлементы корневого - т.е. книги
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
                        {
                            
                           // System.out.println("Rules : " + bookProp.getTextContent());                          
                            Rules.add(bookProp.getTextContent());  
                        }   
                            
                        }
                        // Если нода не текст, то это один из параметров книги - печатаем

                    }
                    //System.out.println("===========>>>>");
                }
            }

        } catch (ParserConfigurationException ex) {
                ex.printStackTrace(System.out);                
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
 
        } catch (SAXException ex) {
                ex.printStackTrace(System.out);                
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
        
        } catch (IllegalArgumentException ex) {
                ex.printStackTrace(System.out);
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();            
                
                
        } catch (IOException ex) {
                 ex.printStackTrace(System.out);                
                AddToFile(ex.getMessage());
                AddToFile(Arrays.toString(ex.getStackTrace()));
                CloseFile();
        }
      
        return Rules;
      
        }        
             

          

}
