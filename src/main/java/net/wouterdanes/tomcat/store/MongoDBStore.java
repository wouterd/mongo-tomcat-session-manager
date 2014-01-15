package net.wouterdanes.tomcat.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import org.apache.catalina.Session;
import org.apache.catalina.session.StoreBase;

/**
 * Implementation of a {@link org.apache.catalina.Store} that persists to MongoDB. It is an alternative to
 * {@link org.apache.catalina.session.JDBCStore} which uses TTL indexes in mongo to handle expiry on sessions.
 */
public class MongoDBStore extends StoreBase {

    private static final String SESSION_ID_FIELD = "_id";
    private static final String SESSION_DATA_FIELD = "session_data";
    private static final String SESSION_EXPIRES_AFTER_FIELD = "expires_after";

    private MongoClient mongo;
    private String uri;
    private String collection;
    private String database;

    protected static String storeName = "MongoDBStore";

    @Override
    public int getSize() throws IOException {
        DBCollection sessionCollection = getSessionCollection();
        return (int) sessionCollection.getCount();
    }

    private DBCollection getSessionCollection() {
        ensureConnection();
        return mongo.getDB(database).getCollection(collection);
    }

    @Override
    public String[] keys() throws IOException {
        DBCollection sessionCollection = getSessionCollection();
        DBCursor sessions = sessionCollection.find(new BasicDBObject(), new BasicDBObject());
        int size = sessions.size();
        String[] keys = new String[size];
        int i = 0;
        while (sessions.hasNext()) {
            DBObject session = sessions.next();
            keys[i++] = (String) session.get(SESSION_ID_FIELD);
        }
        return keys;
    }

    @Override
    public Session load(final String id) throws ClassNotFoundException, IOException {
        manager.getContainer().getLogger().debug("Attempting to load session from mongo with id = " + id);
        DBCollection sessionCollection = getSessionCollection();
        BasicDBObject findObject = new BasicDBObject(SESSION_ID_FIELD, id);
        BasicDBObject fields = new BasicDBObject(SESSION_DATA_FIELD, 1);
        DBObject sessionDocument = sessionCollection.findOne(findObject, fields);
        if (sessionDocument == null) {
            manager.getContainer().getLogger().debug("Cannot find session with id = " + id);
            return null;
        }
        byte[] sessionData = (byte[]) sessionDocument.get(SESSION_DATA_FIELD);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(sessionData);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            manager.getContainer().getLogger().debug("Session found for id = " + id +
                    ", deserializing and returning it.");
            return (Session) ois.readObject();
        }
    }

    @Override
    public void remove(final String id) throws IOException {
        DBCollection sessionCollection = getSessionCollection();
        sessionCollection.remove(new BasicDBObject(SESSION_ID_FIELD, id));
    }

    @Override
    public void clear() throws IOException {
        getSessionCollection().drop();
        ensureIndexes();
    }

    @Override
    public void save(final Session session) throws IOException {
        manager.getContainer().getLogger().debug("Saving session to mongo with id = " + session.getId());
        byte[] sessionData;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(session);
            sessionData = baos.toByteArray();
        }
        BasicDBObject sessionDocument = new BasicDBObject(3);
        if (session.getMaxInactiveInterval() >= 0) {
            Calendar expiresAfter = new GregorianCalendar();
            expiresAfter.add(Calendar.SECOND, session.getMaxInactiveInterval());
            sessionDocument.put(SESSION_EXPIRES_AFTER_FIELD, expiresAfter);
        }
        sessionDocument.put(SESSION_ID_FIELD, session.getId());
        sessionDocument.put(SESSION_DATA_FIELD, sessionData);
        getSessionCollection().insert(sessionDocument);
    }

    @Override
    public void processExpires() {
        // Is handled by MongoDB, don't need to do anything here
        manager.getContainer().getLogger().debug("processExpires() called, not doing anything, because mongo does " +
                "this for us with a TTL index.");
    }

    @Override
    public String getStoreName() {
        return storeName;
    }

    private void ensureConnection() {
        if (mongo == null) {
            manager.getContainer().getLogger().info("Initializing mongo client with uri '" + uri + "'");
            try {
                MongoClientURI mongoClientURI = new MongoClientURI(uri);
                mongo = new MongoClient(mongoClientURI);
                if (database == null) {
                    database = mongoClientURI.getDatabase();
                }
                if (collection == null) {
                    collection = mongoClientURI.getCollection();
                }
            } catch (UnknownHostException e) {
                manager.getContainer().getLogger().error("Please specify a proper host in the uri field", e);
                throw new IllegalArgumentException("Wrong Uri string specified for mongo connection", e);
            }
            ensureIndexes();
        }
    }

    private void ensureIndexes() {
        getSessionCollection().ensureIndex(
                new BasicDBObject(SESSION_EXPIRES_AFTER_FIELD, 1), new BasicDBObject("expireAfterSeconds", 0)
        );
    }

    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        support.firePropertyChange("uri", this.uri, uri);
        this.uri = uri;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(final String collection) {
        support.firePropertyChange("collection", this.collection, collection);
        this.collection = collection;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(final String database) {
        support.firePropertyChange("database", this.database, database);
        this.database = database;
    }
}
