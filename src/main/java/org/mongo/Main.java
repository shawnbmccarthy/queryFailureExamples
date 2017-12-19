package org.mongo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoException;

import org.mongo.reader.MdbReaderExample;
import org.mongo.writer.MdbWriterExample;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final MongoCollection<Document> mCollection;

    /**
     *
     * @param db
     * @param collName
     */
    private Main(MongoDatabase db, String collName){
        logger.debug("constructing execution object");
        mCollection = db.getCollection(collName);
    }

    private void runDemo(){
        logger.debug("creating demo objects");
        MdbReaderExample reader = new MdbReaderExample(mCollection);
        MdbWriterExample writer = new MdbWriterExample(mCollection);

        logger.debug("first starting the writer demo");
        writer.runDemo();

        logger.debug("lets go to the reader demo");
        reader.runDemo();

        logger.info("all done");
    }
    /**
     *
     * @param opts
     */
    private static void printHelp(Options opts){
        HelpFormatter f = new HelpFormatter();
        f.printHelp("java -jar [JARFILE]", opts);
    }

    /**
     *
     * @param args
     */
    public static void main(String [] args) {
        logger.debug("starting query example application");
        Options opts = new Options();
        Option muriOpt = Option.builder("m")
                .longOpt("mongouri")
                .desc("mongo uri to connect to (mongodb://user:pass@host:port,.../db?options)")
                .hasArg()
                .required()
                .build();
        Option collOpt = Option.builder("c")
                .longOpt("collection")
                .desc("name of collection to execute reads/writes against")
                .hasArg()
                .required()
                .build();

        opts.addOption(muriOpt);
        opts.addOption(collOpt);
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(opts, args);
            String muri = cmd.getOptionValue(muriOpt.getOpt());
            String coll = cmd.getOptionValue(collOpt.getOpt());
            logger.debug("attempting demo against: {}, coll: {}", muri, coll);
            MongoClientURI clntUri = new MongoClientURI(muri);
            MongoClient clnt = new MongoClient(clntUri);
            Main m = new Main(clnt.getDatabase(clntUri.getDatabase()), cmd.getOptionValue(collOpt.getOpt()));
            m.runDemo();
        } catch(ParseException pe){
            System.out.println();
            System.out.println(pe.getLocalizedMessage());
            printHelp(opts);
            System.out.println();
            System.exit(1);
        } catch(MongoException me){
            logger.error("problem connecting to mongodb(code:{}): ", me.getCode(), me.getLocalizedMessage());
            System.exit(1);
        } catch(IllegalArgumentException iae){
            logger.error("database name could be invalid: {}", iae.getLocalizedMessage());
        }
    }
}
