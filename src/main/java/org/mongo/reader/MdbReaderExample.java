package org.mongo.reader;

import java.security.SecureRandom;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.bson.Document;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;

/**
 *
 */
public final class MdbReaderExample {
    /**
     *
     */
    private static final Logger log = LoggerFactory.getLogger(MdbReaderExample.class);
    private static final int FIND_BATCH_SZ = 1000;
    private static final SecureRandom rand = new SecureRandom();

    /**
     *
     */
    private final MongoCollection<Document> mColl;

    /**
     * TODO: Catch exceptions & retry!!
     */
    private void findOneDoc(){
        log.info("attempting to find one document {} times", FIND_BATCH_SZ);
        for(int i  = 0; i < FIND_BATCH_SZ; i++){
            /* just some random number from the x field */
            MongoCursor<Document> cur = mColl.find(new Document("x", rand.nextInt(10000))).limit(1).iterator();
            while(cur.hasNext()){
                log.debug("found document with _id: {}", cur.next().get("_id"));
            }
        }
    }

    /**
     *  TODO: catch exceptions & retry!!
     */
    private void findManyDocuments(){
        log.info("attempting to find many documents {} times", FIND_BATCH_SZ);
        for(int i = 0; i < FIND_BATCH_SZ; i++){
            /*
             * Make sure there is an index on {x: 1, iType: 1} ... or it will be slow......
             * silly code to try and search for stuff not in cache ...
             */
            int x = rand.nextInt(1000);
            int count = 0;
            String type = "insertOne";
            if((FIND_BATCH_SZ % (i+1)) == 0){
                type = "insertMany";
            }
            MongoCursor<Document> cur = mColl.find(new Document("x", x).append("iType", type)).iterator();
            while(cur.hasNext()){
                count++;
                log.debug("found document with _id: {}", cur.next().get("_id"));
            }
            log.debug("processed {} documents from find cursor", count);
        }
    }

    /**
     *
     * @param coll
     */
    public MdbReaderExample(MongoCollection coll){
        log.debug("constructing reader object");
        mColl = coll;
    }

    /**
     *
     */
    public void runDemo(){
        log.info("attempting to run reader demo");
        findOneDoc();
        findManyDocuments();
    }
}
