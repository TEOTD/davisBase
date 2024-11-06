package com.neodymium.customdb.models;

import com.neodymium.customdb.error.CustomDbException;
import lombok.extern.slf4j.Slf4j;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

@Slf4j
public record Table<T extends TableRecord>(int numPages, int pageSize, int recordSize, String filePath,
                                           Map<Integer, Page<T>> pages) {

    public void init() {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            byte[] emptyPage = new byte[pageSize];
            for (int i = 0; i < numPages; i++) {
                fos.write(emptyPage);
            }
        } catch (IOException e) {
            log.error("Error while initializing file: ", e);
            throw new CustomDbException("Error while initializing file: " + e.getMessage());
        }
    }

    public void saveToFile() {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
            for (Map.Entry<Integer, Page<T>> entry : pages.entrySet()) {
                int pageNum = entry.getKey();
                byte[] pageData = entry.getValue().toByteArray(pageSize, recordSize);
                randomAccessFile.seek((long) pageNum * pageSize);
                randomAccessFile.write(pageData);
            }
        } catch (IOException e) {
            log.error("Error while saving to file: ", e);
            throw new CustomDbException("Error while saving to file: " + e.getMessage());
        }
    }
}
