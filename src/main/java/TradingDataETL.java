import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import proto.TradingData.TradingDataProto.TradingData;

// Configuration loader class
class ConfigLoader {
    public static Config loadConfig(String filePath) {
        // Load configuration from YAML file (use Jackson or similar library)
        return new Config("/Users/sigmoid/miscFiles/tradingFile.csv", "jdbc:sqlite:/Users/sigmoid/db/tradingETL.db", "/Users/sigmoid/miscFiles/output.proto");
    }
}

// Main ETL pipeline class
public class TradingDataETL {
    public static void main(String[] args) {
        // Step 1: Load configuration
        Config config = ConfigLoader.loadConfig("config.yaml");

        // Step 2: Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("TradingDataETL")
                .master("local[*]") // Run locally with all available cores
                .getOrCreate();

        // Step 3: Extract data
        Dataset<Row> csvData = CsvExtractor.extract(spark, config.getCsvPath());
        Dataset<Row> dbData = DbExtractor.extract(spark, config.getDbUrl());

        //check schema
        csvData.printSchema();
        dbData.printSchema();

        Dataset<Row> csvData1 = spark.read()
                .option("header", "true") // Treat the first row as headers
                .option("inferSchema", "true") // Automatically infer column types
                .csv("/Users/sigmoid/miscFiles/tradingFile.csv");
        // Print schema and sample data
        csvData1.printSchema();
        csvData1.show();

        // Step 4: Combine data
        Dataset<Row> combinedData = csvData.union(dbData);

        // Step 5: Deduplicate data
        Dataset<Row> deduplicatedData = Deduplicator.deduplicate(combinedData);

        // Step 6: Convert to Protocol Buffers and write output
        ProtoConverter.convertAndSave(deduplicatedData, config.getOutputPath());

        // Stop SparkSession
        spark.stop();
    }
}

// Additional helper classes (placeholders for CsvExtractor, DbExtractor, Deduplicator, ProtoConverter)
class CsvExtractor {
    public static Dataset<Row> extract(SparkSession spark, String filePath) {
        return spark.read().option("header", "true").csv(filePath);
    }
}

class DbExtractor {
    public static Dataset<Row> extract(SparkSession spark, String dbUrl) {
        return spark.read()
                .format("jdbc")
                .option("url", dbUrl)
                .option("dbtable", "tradingData") // Replace with your table name
                .load();
    }
}

class Deduplicator {
    public static Dataset<Row> deduplicate(Dataset<Row> data) {
        return data.dropDuplicates("TransactionID");
    }
}


class ProtoConverter {
    public static void convertAndSave(Dataset<Row> data, String outputPath) {
        data.foreach(row -> {
            TradingData proto = convertRowToProto(row);
            saveProtoToFile(proto, outputPath);
        });
    }

    private static TradingData convertRowToProto(Row row) {
        return TradingData.newBuilder()
                .setTransactionID(row.getAs("TransactionID"))
                .setDate(row.getAs("Date"))
                .setTime(row.getAs("Time"))
                .setStockSymbol(row.getAs("StockSymbol"))
                .setAction(row.getAs("Action"))
                .setQuantity(row.getAs("Quantity"))
                .setPrice(row.getAs("Price"))
                .setName(row.getAs("Name"))
                .setKerboros(row.getAs("Kerboros"))
                .setComments(row.getAs("Comments"))
                .build();
    }

    private static void saveProtoToFile(TradingData proto, String outputPath) {
        try (FileOutputStream output = new FileOutputStream(outputPath, true)) {
            proto.writeTo(output);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// Configuration class
class Config {
    private final String csvPath;
    private final String dbUrl;
    private final String outputPath;

    public Config(String csvPath, String dbUrl, String outputPath) {
        this.csvPath = csvPath;
        this.dbUrl = dbUrl;
        this.outputPath = outputPath;
    }

    public String getCsvPath() {
        return csvPath;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getOutputPath() {
        return outputPath;
    }
}
