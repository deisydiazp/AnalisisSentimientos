/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analisissentimientos;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.conversions.Bson;


/**
 *
 * @author Daniel
 */


public class SentimentAnnotated {
    
    public static final String DB_SERVER = "localhost";
    public static final int DB_PORT = 27017;
    public static final String DB_NAME = "Grupo10TwitterAnnotated";
    public static final String COLLECTION_NAME = "DatasetAnnoted_1";
    
    static enum scoreSentiment {
        Negative, Positive, Mixed, Other
    }
    
    
    public static void main(String[] args) {        
        
        //procesarDatasetAnotado();
        consultarDatasetsAnotados();
    }
    
    public static void procesarDatasetAnotado(){
        File archivo = null;
        FileReader fr = null;
        BufferedReader br = null;
        List<Document> jArray = new ArrayList<Document>();
        try{ 
            archivo = new File("D:\\Deisy universidad\\Big Data\\data\\01_debate08_sentiment_tweets\\debate08_sentiment_tweets_procesar.tsv");
            fr = new FileReader(archivo);
            br = new BufferedReader(fr);
            
            // Lectura del fichero
            String linea;
            while ((linea = br.readLine()) != null) {
                String[] csvValues = linea.toString().split("\t");
                
                Document json = new Document();
                json.put("idTweet", csvValues[0]);
                json.put("date", csvValues[1]);
                json.put("content", csvValues[2]);
                json.put("authorName", csvValues[3]);
                json.put("authorNickname", csvValues[4]);
                                
                int j = 1;
                String nameAttribute ="";
                LinkedHashMap<Integer, Integer> ratings = new LinkedHashMap<Integer, Integer>();
                for(int i = 5; i< csvValues.length; i++){
                    nameAttribute = "rating"+j;
                    int rating = Integer.parseInt(csvValues[i]);
                    ratings.put(rating, ratings.containsKey(rating) ? (ratings.get(rating) + 1) : 1);
                    json.put(nameAttribute, csvValues[i]);
                    j++;
                }
                
                int score = 1;
                int maxValueInMap=(Collections.max(ratings.values()));
                for (Map.Entry<Integer, Integer> entry : ratings.entrySet()) {
                    if (entry.getValue() == maxValueInMap) {
                        score = entry.getKey();
                        break;
                    }
                }
                
                switch (score) {
                    case 1:  json.put("sentiment_score_annoted","Negative");
                             break;
                    case 2:  json.put("sentiment_score_annoted","Positive");
                             break;
                    case 3:  json.put("sentiment_score_annoted","Mixed");
                             break;
                    case 4:  json.put("sentiment_score_annoted","Other");
                             break;
                }
                
                //Mongo connection
                jArray.add(json);
            }
            
            MongoDatabase mongoDB = new MongoClient(DB_SERVER, DB_PORT).getDatabase(DB_NAME);
            MongoCollection mongoCollection = mongoDB.getCollection(COLLECTION_NAME);
            mongoCollection.insertMany(jArray);
            fr.close();
            br.close();
            ///tweet.id	pub.date.GMT	content	author.name	author.nickname	rating.1	rating.2	rating.3	rating.4	rating.5	rating.6	rating.7	rating.8
            
        }catch(Exception ex){
            System.out.println("analisissentimientos.SentimentAnnotated.main() Error --->" + ex.toString());
        }
    }
    
    public static void consultarDatasetsAnotados() {

        MongoDatabase mongoDB = new MongoClient(DB_SERVER, DB_PORT).getDatabase(DB_NAME);
        MongoCollection mongoCollection = mongoDB.getCollection(COLLECTION_NAME);
      
        AggregateIterable<Document> result = mongoCollection.aggregate(Arrays.asList(
            new Document("$group", new Document("_id", "$sentiment_score_annoted").append("count", new Document("$sum",1)))
        ));
        
        for (Document doc : result) {
            System.out.println(doc);
        }
    }
    
}
