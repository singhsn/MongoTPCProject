
package com.example.mongotpc;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import java.io.*;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

import org.bson.Document;
import com.mongodb.client.*;

/**
* Program to create a collection, insert JSON objects, and perform simple
* queries on MongoDB.
*/

public class MongoDB {

    /**
     * MongoDB database name
     */
    public static final String DATABASE_NAME = "mydb";

    /**
     * MongoDB collection name
     */
    public static final String COLLECTION_NAME = "data";

    /**
     * Mongo client connection to server
     */
    public MongoClient mongoClient;

    /**
     *  Mongo database
     */
    public MongoDatabase db;

    /**
     *
     * Main method
     * @param args
     * no arguments required
     */
    public static void main(String[] args) throws Exception {
        MongoDB qmongo = new MongoDB();
        qmongo.connect();
        qmongo.load();
        qmongo.loadNest();
        System.out.println(qmongo.query1(1));
        System.out.println(qmongo.query2(1));
        System.out.println(qmongo.query2Nest(1));
        System.out.println(qmongo.query3());
        System.out.println(qmongo.query3Nest());
        System.out.println(MongoDB.toString(qmongo.query4()));
        System.out.println(MongoDB.toString(qmongo.query4Nest()));
    }

    /**
     *
     * Connects to Mongo database and returns database object to manipulate for
     * connection.
     * @return
     * Mongo database
     */
    public MongoDatabase connect() {
        try {
            String url = "mongodb+srv://g24ai1048:P8tdxhLmKxdQZDd0@cluster0.i7h1f8l.mongodb.net/mydb?retryWrites=true&w=majority";
            mongoClient = MongoClients.create(url);
            db = mongoClient.getDatabase(DATABASE_NAME);

            // Add these lines:
            System.out.println("Connected to database: " + db.getName());
            for (String name : db.listCollectionNames()) {
                System.out.println("Found collection: " + name);
            }

        } catch (Exception ex) {
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
        return db;
    }

    /**
     *
     * Loads TPC-H data into MongoDB.
     * @throws Exception
     * if a file I/O or database error occurs
     */
    public void load() throws Exception {
        System.out.println("Starting data loading.....");
        MongoCollection<Document> custCol = db.getCollection("customer");
        MongoCollection<Document> ordCol = db.getCollection("orders");
        custCol.drop();
        ordCol.drop();

        BufferedReader custReader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("customer.tbl")))
        );
        String line;
        while ((line = custReader.readLine()) != null) {

            String[] parts = line.split("\\|");
            Document doc = new Document("custkey", Integer.parseInt(parts[0]))
                    .append("name", parts[1])
                    .append("address", parts[2])
                    .append("nationkey", Integer.parseInt(parts[3]))
                    .append("phone", parts[4])
                    .append("acctbal", new BigDecimal(parts[5]))
                    .append("mktsegment", parts[6])
                    .append("comment", parts[7]);

            //System.out.println("Inserting customer: " + parts[1]);
            custCol.insertOne(doc);
        }
        custReader.close();

        BufferedReader ordReader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("order.tbl")))
        );

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        String line;
        while ((line = ordReader.readLine()) != null) {
            String[] parts = line.split("\\|");
            Document doc = new Document("orderkey", Integer.parseInt(parts[0]))
                    .append("custkey", Integer.parseInt(parts[1]))
                    .append("orderstatus", parts[2])
                    .append("totalprice", new BigDecimal(parts[3]))
                    .append("orderdate", sdf.parse(parts[4]))
                    .append("orderpriority", parts[5])
                    .append("clerk", parts[6])
                    .append("shippriority", Integer.parseInt(parts[7]))
                    .append("comment", parts[8]);
            ordCol.insertOne(doc);
        }
        ordReader.close();
        System.out.println("Data loading done ");
    }

    /**
     *
     * Loads customer and orders TPC-H data into a single collection.
     * @throws Exception
     * if a file I/O or database error occurs
     */
    public void loadNest() throws Exception {
        MongoCollection<Document> col = db.getCollection("custorders");
        col.drop();
        MongoCollection<Document> customers = db.getCollection("customer");
        MongoCollection<Document> orders = db.getCollection("orders");

        MongoCursor<Document> custCursor = customers.find().iterator();
        while (custCursor.hasNext()) {
            Document cust = custCursor.next();
            int custkey = cust.getInteger("custkey");
            List<Document> ordersList = orders.find(eq("custkey", custkey)).into(new ArrayList<>());
            cust.append("orders", ordersList);
            col.insertOne(cust);
        }
    }

    /**
     * Performs a MongoDB query that prints out all data (except for the _id).
     */
    public String query1(int custkey) {
        System.out.println("\nExecuting query 1:");

        MongoCollection<Document> col = db.getCollection("customer");
        Document doc = col.find(eq("custkey", custkey)).projection(fields(include("name"), excludeId())).first();
        return doc != null ? doc.toJson() : "Customer not found.";
    }

    /**
     * * Performs a MongoDB query that returns order date for a given order id using
     * * the orders collection.
     */
    public String query2(int orderId) {
        System.out.println("\nExecuting query 2:");

        MongoCollection<Document> col = db.getCollection("orders");
        Document doc = col.find(eq("orderkey", orderId)).projection(fields(include("orderdate"), excludeId())).first();
        return doc != null ? doc.toJson() : "Order not found.";
    }

    /**
     * * Performs a MongoDB query that returns order date for a given order id using
     * * the custorders collection.
     */
    public String query2Nest(int orderId) {
        System.out.println("\nExecuting query 2 nested:");

        MongoCollection<Document> col = db.getCollection("custorders");
        Document match = col.find(com.mongodb.client.model.Filters.elemMatch("orders", eq("orderkey", orderId)))
                .projection(fields(include("orders"), excludeId())).first();

        if (match != null) {
            List<Document> orders = (List<Document>) match.get("orders");
            for (Document order : orders) {
                if (order.getInteger("orderkey") == orderId)
                    return order.get("orderdate").toString();
            }
        }

        return "Order not found in nested.";
    }

    /**
     * * Performs a MongoDB query that returns the total number of orders using the
     * * orders collection.
     */
    public long query3() {
        System.out.println("\nExecuting query 3:");

        MongoCollection<Document> col = db.getCollection("orders");
        return col.countDocuments();
    }

    /**
     * * Performs a MongoDB query that returns the total number of orders using the
     * * custorders collection.
     */
    public Integer query3Nest() {
        System.out.println("\nExecuting query 3 nested:");

        MongoCollection<Document> col = db.getCollection("custorders");
        AggregateIterable<Document> result = col.aggregate(List.of(
                new Document("$project", new Document("orderCount", new Document("$size", "$orders"))),
                new Document("$group", new Document("_id", null).append("total", new Document("$sum", "$orderCount")))
        ));
        Document doc = result.first();
        return doc != null ? doc.getInteger("total") : 0;
    }

    /**
     * * Performs a MongoDB query that returns the top 5 customers based on total
     * * order amount using the customer and orders collections.
     */
    public MongoCursor<Document> query4() {
        System.out.println("\nExecuting query 4:");

        MongoCollection<Document> orders = db.getCollection("orders");
        MongoCollection<Document> customers = db.getCollection("customer");

        AggregateIterable<Document> result = orders.aggregate(List.of(
                new Document("$group", new Document("_id", "$custkey")
                        .append("totalAmount", new Document("$sum", "$totalprice"))),
                new Document("$sort", new Document("totalAmount", -1)),
                new Document("$limit", 5),
                new Document("$lookup", new Document("from", "customer")
                        .append("localField", "_id")
                        .append("foreignField", "custkey")
                        .append("as", "customerInfo")),
                new Document("$unwind", "$customerInfo"),
                new Document("$project", new Document("custkey", "$_id")
                        .append("name", "$customerInfo.name")
                        .append("totalAmount", 1).append("_id", 0))
        ));

        return result.iterator();
    }

    /**
     * * Performs a MongoDB query that returns the top 5 customers based on total
     * * order amount using the custorders collection.
     */
    public MongoCursor<Document> query4Nest() {
        System.out.println("\nExecuting query 4 nested:");

        MongoCollection<Document> col = db.getCollection("custorders");

        AggregateIterable<Document> result = col.aggregate(List.of(
                new Document("$project", new Document("custkey", 1).append("name", 1).append("orders", 1)),
                new Document("$addFields", new Document("totalAmount", new Document("$sum", "$orders.totalprice"))),
                new Document("$sort", new Document("totalAmount", -1)),
                new Document("$limit", 5),
                new Document("$project", new Document("_id", 0).append("custkey", 1).append("name", 1).append("totalAmount", 1))
        ));

        return result.iterator();
    }

    /**
     *
     * Returns the Mongo database being used.
     * @return
     * Mongo database
     */
    public MongoDatabase getDb() {
        return db;
    }

    /**
     *
     * Outputs a cursor of MongoDB results in string form.
     * @param cursor
     * Mongo cursor
     * @return
     * results as a string
     */
    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder("Rows:\n");
        int count = 0;
        if (cursor != null) {
            while (cursor.hasNext()) {
                buf.append(cursor.next().toJson()).append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: ").append(count);
        return buf.toString();
    }
}
