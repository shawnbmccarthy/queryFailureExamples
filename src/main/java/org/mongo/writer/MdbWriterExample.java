package org.mongo.writer;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoBulkWriteException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;

/**
 *
 */
public class MdbWriterExample {
    /**
     *
     */
    private static final Logger log = LoggerFactory.getLogger(MdbWriterExample.class);

    /**
     *
     */
    private static final int BATCH_SZ = 1000;

    /**
     *
     */
    private static final int NUM_OF_LOOPS = 1000;

    /**
     *
     */
    private static final int MAX_RETRIES = 10;

    /**
     *
     */
    private final MongoCollection<Document> mColl;

    /**
     *
     */
    private void insertOneLoop(){
        log.debug("attempting to insert one in a loop of {}", BATCH_SZ);

        long start = System.currentTimeMillis();
        long interm = start;
        int cur = 0;
        for(int i = 0; i < BATCH_SZ * NUM_OF_LOOPS; i++){
            Document toInsert = new Document("x", i).append("text", "hello from insertOne").append("iType", "insertOne");
            try {
                mColl.insertOne(toInsert);
                log.debug("inserted one document successfully, generated _id: {}", toInsert.get("_id"));
            } catch(MongoException me){
                log.error("caught writing exception: {}", me.getLocalizedMessage());
                /*
                 * here is an example of some sort of retry logic
                 * enter a for loop to do the retry pausing for some number of seconds
                 */
                int attempt = 1;
                boolean success = false;
                while(attempt <= MAX_RETRIES){
                    try {
                        log.warn("attempting retry in {} ms(s)", (attempt*100));
                        Thread.sleep((attempt * 100));
                        mColl.insertOne(toInsert);
                        attempt = MAX_RETRIES + 1;
                        success = true;
                    } catch(InterruptedException ie){
                        log.debug("InterruptedException caught, don't sweat it: {}", ie.getLocalizedMessage());
                    } catch(MongoException m) {
                        log.debug("attempt {} failed will try again ({})", attempt, m.getLocalizedMessage());
                        attempt++;
                    }
                }
                if(! success){
                    /* write to log or something the data we want to insert */
                    log.error("could not write data: {}", toInsert.get("_id"));
                } else {
                    log.info("successfully installed after failure");
                }
            }
            /*
             * after all exception info to make sure we account for the error times too!
             */
            if((i % BATCH_SZ) == 0){
                cur = i - cur;
                long bEnd = System.currentTimeMillis();
                log.info("inserted {} docs in {}ms", cur, (bEnd - interm));
                interm = System.currentTimeMillis();
            }
        }
        log.info("insertOne executed in: {}ms", (System.currentTimeMillis() - start));
    }

    /**
     *
     */
    private void insertManyLoop(){
        log.debug("attempting batch insert {} times", BATCH_SZ);
        long start = System.currentTimeMillis();
        long interm = start;
        List<Document> toInsert = new ArrayList<>(BATCH_SZ);
        for(int i = 0; i < NUM_OF_LOOPS * BATCH_SZ; i++){
            Document doc = new Document("x", i).append("text", "hello from batch").append("iType", "insertMany");
            toInsert.add(doc);
            if(toInsert.size() >= BATCH_SZ){
                insertMany(toInsert);
                log.info("inserted {} docs in {}ms", toInsert.size(), (System.currentTimeMillis()-interm));
                interm = System.currentTimeMillis();
                toInsert.clear();
            }
        }
        if(toInsert.size() > 0){
            insertMany(toInsert);
            log.info("inserted {} docs in {}ms", toInsert.size(), (System.currentTimeMillis()-interm));
            toInsert.clear();
        }
        log.info("inserted {} docs in {}ms", (NUM_OF_LOOPS * BATCH_SZ), (System.currentTimeMillis()-start));
    }

    private void insertMany(List<Document> list){
        try {
            mColl.insertMany(list);
        } catch(MongoException me){
            /* this exception might be for something special that we want to check before moving on */
            log.error("caught exception: {}", me.getLocalizedMessage());
            /* unordered bulk, we would need to check for each of the documents */
            for(Document doc : list){
                ObjectId id = doc.getObjectId("_id");
                if(mColl.count(new Document("_id", id)) == 1){
                    list.remove(doc);
                }
            }
            /* after the count - attempt the install on the new list */
            log.error("list size now: {}", list.size());

        }
    }

    /**
     *
     * @param coll
     */
    public MdbWriterExample(MongoCollection coll){
        log.debug("creating writer object");
        mColl = coll;
    }

    /**
     *
     */
    public void runDemo(){
        insertOneLoop();
        insertManyLoop();
    }
}
