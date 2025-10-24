// ParallelEmbeddedDocumentExtractorFactory.java
package org.apache.tika.parallel;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ParallelEmbeddedDocumentExtractor - מעבד רכיבים מוטמעים במקביל
 * 
 * עיבוד מקבילי של תמונות, קבצים מצורפים, ומסמכים מוטמעים אחרים
 * ללא שינוי קוד המקור של Tika.
 * 
 * @author Your Name
 * @version 1.0.0
 */
public class ParallelEmbeddedDocumentExtractor extends ParsingEmbeddedDocumentExtractor {
    
    // הגדרות ברירת מחדל
    private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
    private static final long DEFAULT_MAX_SIZE_MB = 50;
    private static final long DEFAULT_TIMEOUT_SECONDS = 300;
    
    // ThreadPool לעיבוד מקבילי
    private final ExecutorService executorService;
    
    // רשימת המשימות הפעילות
    private final List<Future<EmbeddedResult>> activeTasks;
    
    // counter לתיוג רכיבים
    private final AtomicInteger embeddedCounter;
    
    // הגדרות
    private final int numThreads;
    private final long maxSizeBytes;
    private final long timeoutSeconds;
    
    // לוג
    private final boolean debug;
    
    // Parser instance
    private final Parser parser;
    
    /**
     * Constructor
     */
    public ParallelEmbeddedDocumentExtractor(ParseContext context) {
        super(context);
        
        // שמירת parser מה-context
        this.parser = context.get(Parser.class);
        
        // קריאת הגדרות מ-environment variables או system properties
        this.numThreads = getConfigInt("tika.parallel.threads", DEFAULT_THREADS);
        long maxSizeMB = getConfigLong("tika.parallel.maxSizeMB", DEFAULT_MAX_SIZE_MB);
        this.maxSizeBytes = maxSizeMB * 1024 * 1024;
        this.timeoutSeconds = getConfigLong("tika.parallel.timeoutSeconds", DEFAULT_TIMEOUT_SECONDS);
        this.debug = getConfigBoolean("tika.parallel.debug", false);
        
        // יצירת ThreadPool
        this.executorService = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            private final AtomicInteger threadCounter = new AtomicInteger(1);
            
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "tika-parallel-" + threadCounter.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
        
        this.activeTasks = new CopyOnWriteArrayList<>();
        this.embeddedCounter = new AtomicInteger(0);
        
        if (debug) {
            System.out.println("[ParallelExtractor] Initialized with " + numThreads + " threads");
            System.out.println("[ParallelExtractor] Max size: " + maxSizeMB + "MB");
        }
    }
    
    /**
     * עיבוד רכיב מוטמע - יבצע במקביל
     */
    @Override
    public boolean shouldParseEmbedded(Metadata metadata) {
        // בדיקת גודל
        String lengthStr = metadata.get(Metadata.CONTENT_LENGTH);
        if (lengthStr != null) {
            try {
                long length = Long.parseLong(lengthStr);
                if (length > maxSizeBytes) {
                    if (debug) {
                        System.out.println("[ParallelExtractor] Skipping large embedded: " + 
                            metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY) + " (" + length + " bytes)");
                    }
                    return false;
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        
        return super.shouldParseEmbedded(metadata);
    }
    
    /**
     * עיבוד רכיב מוטמע - הגרסה המקבילית
     */
    @Override
    public void parseEmbedded(
            InputStream stream,
            ContentHandler handler,
            Metadata metadata,
            boolean outputHtml) throws SAXException, IOException {
        
        // יצירת ID ייחודי
        int embeddedId = embeddedCounter.incrementAndGet();
        
        // שמירת ערכים כ-final למשתנה הlambda
        final String resourceName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY) != null 
            ? metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY) 
            : "embedded-" + embeddedId;
        final Metadata metadataFinal = metadata;
        final boolean outputHtmlFinal = outputHtml;
        final ContentHandler handlerFinal = handler;
        
        if (debug) {
            System.out.println("[ParallelExtractor] Processing embedded #" + embeddedId + ": " + resourceName);
        }
        
        // קריאת הstream לזיכרון (צריך לשמור אותו לעיבוד async)
        byte[] data = readStreamToBytes(stream);
        
        // יצירת משימה לעיבוד מקבילי
        Future<EmbeddedResult> future = executorService.submit(() -> {
            return processEmbeddedDocument(embeddedId, resourceName, data, metadataFinal, outputHtmlFinal, handlerFinal);
        });
        
        // שמירת המשימה
        activeTasks.add(future);
        
        // אם הגענו למספר גדול של משימות - נחכה לחלק מהן
        if (activeTasks.size() > numThreads * 2) {
            waitForSomeTasks();
        }
    }
    
    /**
     * עיבוד מסמך מוטמע (מתבצע ב-thread נפרד)
     */
    private EmbeddedResult processEmbeddedDocument(
            int embeddedId,
            String resourceName,
            byte[] data,
            Metadata metadata,
            boolean outputHtml,
            ContentHandler handler) {
        
        long startTime = System.currentTimeMillis();
        EmbeddedResult result = new EmbeddedResult();
        result.id = embeddedId;
        result.resourceName = resourceName;
        result.metadata = metadata;
        
        try {
            // יצירת InputStream מהdata
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            TikaInputStream tikaStream = TikaInputStream.get(inputStream);
            
            // יצירת handler לתוצאה
            BodyContentHandler contentHandler = new BodyContentHandler(-1);
            
            // Parse context
            ParseContext context = new ParseContext();
            
            // שימוש ב-parser המקורי
            Parser parserToUse = this.parser;
            if (parserToUse == null) {
                // fallback - ננסה ליצור parser אוטומטית
                parserToUse = new org.apache.tika.parser.AutoDetectParser();
            }
            context.set(Parser.class, parserToUse);
            
            // עיבוד המסמך
            parserToUse.parse(tikaStream, contentHandler, metadata, context);
            
            result.content = contentHandler.toString();
            result.success = true;
            
            // כתיבה ל-handler המקורי (חשוב!)
            if (handler != null && result.content != null && !result.content.isEmpty()) {
                try {
                    handler.characters(result.content.toCharArray(), 0, result.content.length());
                } catch (SAXException e) {
                    if (debug) {
                        System.err.println("[ParallelExtractor] Error writing to handler: " + e.getMessage());
                    }
                }
            }
            
            if (debug) {
                long elapsed = System.currentTimeMillis() - startTime;
                System.out.println("[ParallelExtractor] Completed #" + embeddedId + 
                    " (" + resourceName + ") in " + elapsed + "ms - " + result.content.length() + " chars");
            }
            
        } catch (Exception e) {
            result.success = false;
            result.error = e.getMessage();
            
            if (debug) {
                System.err.println("[ParallelExtractor] Error processing #" + embeddedId + 
                    " (" + resourceName + "): " + e.getMessage());
            }
        }
        
        result.processingTimeMs = System.currentTimeMillis() - startTime;
        return result;
    }
    
    /**
     * המתנה לחלק מהמשימות
     */
    private void waitForSomeTasks() {
        List<Future<EmbeddedResult>> toRemove = new ArrayList<>();
        
        for (Future<EmbeddedResult> task : activeTasks) {
            if (task.isDone()) {
                toRemove.add(task);
            }
        }
        
        activeTasks.removeAll(toRemove);
    }
    
    /**
     * המתנה לכל המשימות (קוראים לזה בסוף)
     */
    public List<EmbeddedResult> waitForAllTasks() {
        List<EmbeddedResult> results = new ArrayList<>();
        
        if (debug) {
            System.out.println("[ParallelExtractor] Waiting for " + activeTasks.size() + " tasks...");
        }
        
        for (Future<EmbeddedResult> task : activeTasks) {
            try {
                EmbeddedResult result = task.get(timeoutSeconds, TimeUnit.SECONDS);
                results.add(result);
            } catch (TimeoutException e) {
                if (debug) {
                    System.err.println("[ParallelExtractor] Task timeout");
                }
            } catch (Exception e) {
                if (debug) {
                    System.err.println("[ParallelExtractor] Task error: " + e.getMessage());
                }
            }
        }
        
        activeTasks.clear();
        
        if (debug) {
            System.out.println("[ParallelExtractor] All tasks completed. Results: " + results.size());
        }
        
        return results;
    }
    
    /**
     * סגירת ה-ExecutorService
     */
    public void shutdown() {
        waitForAllTasks();
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    // Helper methods
    
    private byte[] readStreamToBytes(InputStream stream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[8192];
        int nRead;
        
        while ((nRead = stream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        
        return buffer.toByteArray();
    }
    
    private int getConfigInt(String key, int defaultValue) {
        // ניסיון מ-system property
        String value = System.getProperty(key);
        if (value == null) {
            // ניסיון מ-environment variable
            value = System.getenv(key.toUpperCase().replace('.', '_'));
        }
        
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        
        return defaultValue;
    }
    
    private long getConfigLong(String key, long defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key.toUpperCase().replace('.', '_'));
        }
        
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        
        return defaultValue;
    }
    
    private boolean getConfigBoolean(String key, boolean defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key.toUpperCase().replace('.', '_'));
        }
        
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }
    
    /**
     * תוצאת עיבוד רכיב מוטמע
     */
    public static class EmbeddedResult {
        public int id;
        public String resourceName;
        public Metadata metadata;
        public String content;
        public boolean success;
        public String error;
        public long processingTimeMs;
        
        @Override
        public String toString() {
            return String.format("EmbeddedResult[id=%d, name=%s, success=%b, time=%dms]",
                id, resourceName, success, processingTimeMs);
        }
    }
}




package org.apache.tika.parallel;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentExtractorFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

/**
 * Factory ליצירת ParallelEmbeddedDocumentExtractor
 * Tika תשתמש בזה דרך tika-config.xml
 */
public class ParallelEmbeddedDocumentExtractorFactory implements EmbeddedDocumentExtractorFactory {
    
    @Override
    public EmbeddedDocumentExtractor newInstance(Metadata metadata, ParseContext context) {
        return new ParallelEmbeddedDocumentExtractor(context);
    }
}
