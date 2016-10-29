package analisissentimientos;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Generics;
import java.util.ArrayList;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * A wrapper class which creates a suitable pipeline for the sentiment
 * model and processes raw text.
 *<br>
 * The main program has the following options: <br>
 * <code>-parserModel</code> Which parser model to use, defaults to englishPCFG.ser.gz <br>
 * <code>-sentimentModel</code> Which sentiment model to use, defaults to sentiment.ser.gz <br>
 * <code>-file</code> Which file to process. <br>
 * <code>-fileList</code> A comma separated list of files to process. <br>
 * <code>-stdin</code> Read one line at a time from stdin. <br>
 * <code>-output</code> pennTrees: Output trees with scores at each binarized node.  vectors: Number tree nodes and print out the vectors.  probabilities: Output the scores for different labels for each node. Defaults to printing just the root. <br>
 * <code>-filterUnknown</code> remove unknown trees from the input.  Only applies to TREES input, in which case the trees must be binarized with sentiment labels <br>
 * <code>-help</code> Print out help <br>
 *
 * @author John Bauer
 */
public class SentimentPipeline {

    public static final String DB_SERVER = "localhost";
    public static final int DB_PORT = 27017;
    /*public static final String DB_NAME = "Grupo10Twitter";
    public static final String COLLECTION_NAME = "OriginalData";*/
    public static final String DB_NAME = "Grupo10TwitterAnnotated";
    public static final String COLLECTION_NAME = "DatasetAnnoted_3";
    
    // private final MongoDatabase mongoDB;
    
    static enum Input {
        TEXT, TREES
    }

    private SentimentPipeline() {
    
    } // static methods

  /**
   * Reads an annotation from the given filename using the requested input.
   */
    public static List<Annotation> getAnnotations(StanfordCoreNLP tokenizer, Input inputFormat, String textTweet, boolean filterUnknown) {
        Annotation annotation = new Annotation(textTweet);
        tokenizer.annotate(annotation);
        List<Annotation> annotations = Generics.newArrayList();
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
          Annotation nextAnnotation = new Annotation(sentence.get(CoreAnnotations.TextAnnotation.class));
          nextAnnotation.set(CoreAnnotations.SentencesAnnotation.class, Collections.singletonList(sentence));
          annotations.add(nextAnnotation);
        }
        return annotations;
    }

    public static void main(String[] args) throws IOException {        
        // Mongo connection
        MongoDatabase mongoDB = new MongoClient(DB_SERVER, DB_PORT).getDatabase(DB_NAME);
        MongoCollection mongoCollection = mongoDB.getCollection(COLLECTION_NAME);
        
        Bson filterInicial = Filters.exists("sentiment_score", false);
        //Bson filterInicial = Filters.all("id",548);// ("sentiment_score", false);
        List<Document> documents = (List<Document>) mongoCollection.find(filterInicial).into(new ArrayList<Document>());
        
        for(Document document : documents){
            System.out.println(document);
            
            int id_str = Integer.parseInt(document.get("id_str").toString());
            String textTweet = document.get("text").toString();
            String sentiment_score = getSentimentScore(textTweet);
            Bson filter = new Document("id_str", id_str);
            Bson newValue = new Document("sentiment_score", sentiment_score);
            Bson updateOperationDocument = new Document("$set", newValue);
            mongoCollection.updateOne(filter, updateOperationDocument);
        }
    }
    
    private static String getSentimentScore(String textTweet) {
        
        String parserModel = null;
        
        boolean filterUnknown = false;

        parserModel = "edu/stanford/nlp/models/lexparser/spanishPCFG.ser.gz";
        
        Input inputFormat = Input.TEXT;

        Properties pipelineProps = new Properties();
        Properties tokenizerProps = null;

        pipelineProps.setProperty("parse.model", parserModel);
        pipelineProps.setProperty("annotators", "parse, sentiment");
        pipelineProps.setProperty("enforceRequirements", "false");
        tokenizerProps = new Properties();
        tokenizerProps.setProperty("annotators", "tokenize, ssplit");

        StanfordCoreNLP tokenizer = (tokenizerProps == null) ? null : new StanfordCoreNLP(tokenizerProps);
        StanfordCoreNLP pipeline = new StanfordCoreNLP(pipelineProps);

        String valor = "";
        List<Annotation> annotations = getAnnotations(tokenizer, inputFormat, textTweet, filterUnknown);
        for (Annotation annotation : annotations) {
            pipeline.annotate(annotation);

            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                System.out.println(sentence);
                valor = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
                System.out.println("valor: "+valor);
                //System.out.println("  " + sentence.get(SentimentCoreAnnotations.SentimentClass.class));
            }
        }
        
        return valor;
    }

}
