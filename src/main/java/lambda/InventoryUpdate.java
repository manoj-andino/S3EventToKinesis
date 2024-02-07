package lambda;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import org.json.JSONObject;
import org.json.JSONTokener;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import utils.MapperUtil;
import xml.Inventory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public class InventoryUpdate implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        LambdaLogger logger = context.getLogger();
        try {
            List<S3EventNotification.S3EventNotificationRecord> s3EventRecords = s3Event.getRecords();
            S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventRecords.get(0);
            String bucketName = s3EventNotificationRecord.getS3().getBucket().getName();
            String key = s3EventNotificationRecord.getS3().getObject().getKey();

            AmazonS3 amazonS3Client = AmazonS3ClientBuilder.defaultClient();
            String objectAsString = amazonS3Client.getObjectAsString(bucketName, key);
            Inventory inventory = MapperUtil.mapXmlToInventoryObject(objectAsString);
            String payload = MapperUtil.writeAsString(inventory);
            logger.log("Payload " + payload, LogLevel.DEBUG);

            software.amazon.awssdk.services.glue.model.DataFormat dataFormat = DataFormat.AVRO;
            AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
            GlueSchemaRegistryConfiguration gsrConfig = new GlueSchemaRegistryConfiguration("ap-south-1");
            gsrConfig.setRegistryName("InventoryEventSchemas");
            gsrConfig.setSchemaAutoRegistrationEnabled(true);
            GlueSchemaRegistrySerializer glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializerImpl(
                    awsCredentialsProvider, gsrConfig);
            GlueSchemaRegistryDataFormatSerializer dataFormatSerializer = new GlueSchemaRegistrySerializerFactory()
                    .getInstance(dataFormat, gsrConfig);


            if(this.getClass().getClassLoader() == null) {
                logger.log("This class has been loaded by bootstrap class loader...");
            }
            InputStream in = getClass().getResourceAsStream("/schema.json");

            if(in == null) {
                logger.log("Input Stream is null", LogLevel.DEBUG);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            StringBuilder text = new StringBuilder();
            String line = reader.readLine();
            while (line != null) {
                logger.log(line, LogLevel.DEBUG);
                text.append(line).append("\n");
                line = reader.readLine();
            }
            reader.close();
            String content = text.toString();

            logger.log("Content is " + content, LogLevel.DEBUG);
            String schemaString = new JSONObject(new JSONTokener(content)).toString();
            logger.log("Schema String is " + schemaString);
            JsonDataWithSchema jsonRecord1 = JsonDataWithSchema.builder(schemaString, payload).build();
            Schema gsrSchema =
                    new Schema(dataFormatSerializer.getSchemaDefinition(jsonRecord1), dataFormat.name(), "InventoryEvent");

            byte[] serializedBytes = dataFormatSerializer.serialize(jsonRecord1);

            byte[] gsrEncodedBytes = glueSchemaRegistrySerializer.encode("inventory-update-kinesis-spike", gsrSchema, serializedBytes);


            AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.defaultClient();
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamARN("arn:aws:kinesis:ap-south-1:417610864161:stream/inventory-update-kinesis-spike");
            putRecordRequest.setStreamName("inventory-update-kinesis-spike");
            putRecordRequest.setData(ByteBuffer.wrap(gsrEncodedBytes));
            putRecordRequest.setPartitionKey("partitionKey-1");
            PutRecordResult result = kinesisClient.putRecord(putRecordRequest);
            logger.log("Successfully inserted record into shard " + result.getShardId(), LogLevel.DEBUG);
            return "success";
        } catch (Exception e) {
            logger.log("Request failed with error " + e);
            return "failure";
        }

    }
}