package com.google.cloud.pso.bq_snapshot_manager.services.tracking;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

public class GCSObjectTracker implements ObjectTracker {

    private String bucketName;

    public GCSObjectTracker(String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public void trackObjects(List<Object> objects, String objectPrefix) throws IOException {

        // Initialize GSON
        Gson gson = new Gson();

        // Initialize Google Cloud Storage client
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // Create a byte array output stream to write JSON data
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Create a buffered writer for writing newline-delimited JSON
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(baos));

        // Convert each object to JSON and write it to the writer with a newline
        for (Object object : objects) {
            String json = gson.toJson(object);
            writer.write(json);
            writer.newLine();
        }

        // Close the writer and output stream
        writer.close();
        baos.close();

        String objectName = String.format("%s/data.json", objectPrefix);

        // Create the blob information
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).setContentType("application/json").build();

        // Create the blob
        storage.create(blobInfo, baos.toByteArray());
    }
}
