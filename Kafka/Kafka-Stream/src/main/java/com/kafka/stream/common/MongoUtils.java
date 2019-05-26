package com.kafka.stream.common;

import com.kafka.stream.processor.ProcessorSupplierFactory;
import com.kafka.stream.util.PropertyReaderUtil;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoUtils {
    private static PropertyReaderUtil reader = new PropertyReaderUtil(); //读取配置文件
    public static final Logger logger = LoggerFactory.getLogger(MongoUtils.class);
    private static final String DB_NAME, COLLECTION_NAME, SERVER_IP;
    private static int PORT;
    private static MongoClient client;
    private static MongoDatabase db;
    private static MongoCollection<Document> collection;

    static {
        Map<String, String> serverMap = reader.readPropertyFile("mongodb.properties");
        SERVER_IP = serverMap.get("host");
//        数据库名称
        DB_NAME = serverMap.get("db.name");
//        数据集合名称
        COLLECTION_NAME = serverMap.get("collection.name");
//        端口
        PORT = Integer.parseInt(serverMap.get("db.port"));
        connect(DB_NAME, COLLECTION_NAME, SERVER_IP, PORT);
    }


    public synchronized static MongoClient getInstance(List<ServerAddress> serverAddress) {
        if (client == null) {
            client = new MongoClient(serverAddress);
        }
        return client;
    }

    public synchronized static void mongodbClose() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * 连接数据库
     *
     * @param databaseName   数据库名称
     * @param collectionName 集合名称
     * @param server         主机名
     * @param port           端口号
     */
    public synchronized static void connect(String databaseName, String collectionName, String server, int port) {
        try {
            if (client == null) {
                client = new MongoClient(server, port);
            }
            if (db == null) {
                db = client.getDatabase(databaseName);
            }
            if (collection == null) {
                collection = db.getCollection(collectionName);
            }
        } catch (Exception e) {
            logger.error("[Connect Error] " + e);
            throw e;
        }
    }

    /**
     * 插入一个文档
     *
     * @param document 文档
     */
    public static void mongodbInsert(Document document) {
        try {
            collection.insertOne(document);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询所有文档
     *
     * @return 所有文档集合
     */
    public static List<Document> mongodbFindAll() {
        List<Document> results = new ArrayList<Document>();
        try {
            FindIterable<Document> iterables = collection.find();
            MongoCursor<Document> cursor = iterables.iterator();
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongodbClose();
            return results;
        }
    }

    /**
     * 根据条件查询
     *
     * @param filter 查询条件 //注意Bson的几个实现类，BasicDBObject, BsonDocument,
     *               BsonDocumentWrapper, CommandResult, Document, RawBsonDocument
     * @return 返回集合列表
     */
    public static List<Document> findBy(Bson filter) {
        List<Document> results = new ArrayList<Document>();
        try {
            FindIterable<Document> iterables = collection.find(filter);
            MongoCursor<Document> cursor = iterables.iterator();
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongodbClose();
            return results;
        }

    }

    /**
     * 更新查询到的第一个
     *
     * @param filter 查询条件
     * @param update 更新文档
     * @return 更新结果
     */
    public static UpdateResult updateOne(Bson filter, Bson update) {
            UpdateResult result = collection.updateOne(filter, update);
            mongodbClose();
            return result;
    }

    /**
     * 更新查询到的所有的文档
     *
     * @param filter 查询条件
     * @param update 更新文档
     * @return 更新结果
     */
    public static UpdateResult updateMany(Bson filter, Bson update) {
        UpdateResult result = collection.updateMany(filter, update);
        return result;
    }

    /**
     * 更新一个文档, 结果是replacement是新文档，老文档完全被替换
     *
     * @param filter      查询条件
     * @param replacement 跟新文档
     */
    public static void replace(Bson filter, Document replacement) {
        collection.replaceOne(filter, replacement);
    }

    /**
     * 根据条件删除一个文档
     *
     * @param filter 查询条件
     */
    public static void deleteOne(Bson filter) {
        collection.deleteOne(filter);
    }

    /**
     * 根据条件删除多个文档
     *
     * @param filter 查询条件
     */
    public static void deleteMany(Bson filter) {
        collection.deleteMany(filter);
    }
}
