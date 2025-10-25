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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Parallel Embedded Document Extractor
 * 
 * Processes embedded documents (images, attachments, etc.) in parallel
 * while preserving their original order in the output.
 * 
 * @version 1.0.0
 */
public class ParallelEmbeddedDocumentExtractor extends ParsingEmbeddedDocumentExtractor {
    
    private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
    private static final long DEFAULT_MAX_SIZE_MB = 50;
    private static final long DEFAULT_TIMEOUT_SECONDS = 300;
    
    private final ExecutorService executorService;
    private final List<Future<EmbeddedResult>> activeTasks;
    private final ConcurrentLinkedQueue<Future<EmbeddedResult>> orderedTasks;
    private final AtomicInteger embeddedCounter;
    private final int numThreads;
    private final long maxSizeBytes;
    private final long timeoutSeconds;
    private final boolean debug;
    private final Parser parser;
    private ContentHandler mainHandler;
    
    public ParallelEmbeddedDocumentExtractor(ParseContext context) {
        super(context);
        
        this.parser = context.get(Parser.class);
        this.numThreads = getConfigInt("tika.parallel.threads", DEFAULT_THREADS);
        long maxSizeMB = getConfigLong("tika.parallel.maxSizeMB", DEFAULT_MAX_SIZE_MB);
        this.maxSizeBytes = maxSizeMB * 1024 * 1024;
        this.timeoutSeconds = getConfigLong("tika.parallel.timeoutSeconds", DEFAULT_TIMEOUT_SECONDS);
        this.debug = getConfigBoolean("tika.parallel.debug", false);
        
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
        this.orderedTasks = new ConcurrentLinkedQueue<>();
        this.embeddedCounter = new AtomicInteger(0);
        
        if (debug) {
            System.out.println("[ParallelExtractor] Initialized with " + numThreads + " threads");
            System.out.println("[ParallelExtractor] Max size: " + maxSizeMB + "MB");
        }
    }
    
    @Override
    public boolean shouldParseEmbedded(Metadata metadata) {
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
     * Parse embedded document in parallel while preserving order.
     * Processing happens concurrently, but output is written in original order.
     */
    @Override
    public void parseEmbedded(
            InputStream stream,
            ContentHandler handler,
            Metadata metadata,
            boolean outputHtml) throws SAXException, IOException {
        
        int embeddedId = embeddedCounter.incrementAndGet();
        
        final String resourceName = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY) != null 
            ? metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY) 
            : "embedded-" + embeddedId;
        final Metadata metadataFinal = metadata;
        final boolean outputHtmlFinal = outputHtml;
        final ContentHandler handlerFinal = handler;
        
        if (this.mainHandler == null && handler != null) {
            this.mainHandler = handler;
        }
        
        if (debug) {
            System.out.println("[ParallelExtractor] Submitting embedded #" + embeddedId + ": " + resourceName);
        }
        
        byte[] data = readStreamToBytes(stream);
        
        Future<EmbeddedResult> future = executorService.submit(() -> {
            return processEmbeddedDocument(embeddedId, resourceName, data, metadataFinal, outputHtmlFinal, null);
        });
        
        orderedTasks.add(future);
        activeTasks.add(future);
        
        writeCompletedTasksInOrder(handlerFinal);
        
        if (activeTasks.size() > numThreads * 3) {
            waitForSomeTasks();
            writeCompletedTasksInOrder(handlerFinal);
        }
    }
    
    /**
     * Finish processing and write all remaining results.
     */
    public void finishProcessing() {
        if (debug) {
            System.out.println("[ParallelExtractor] finishProcessing() called - " + orderedTasks.size() + " tasks remaining");
        }
        
        try {
            waitForAllTasks(this.mainHandler);
        } catch (SAXException e) {
            if (debug) {
                System.err.println("[ParallelExtractor] Error in finishProcessing: " + e.getMessage());
            }
        }
    }
    
    /**
     * Write completed tasks in their original order.
     * Waits briefly for next task if needed.
     */
    private void writeCompletedTasksInOrder(ContentHandler handler) throws SAXException {
        while (!orderedTasks.isEmpty()) {
            Future<EmbeddedResult> nextTask = orderedTasks.peek();
            
            if (nextTask == null) {
                orderedTasks.poll();
                continue;
            }
            
            if (!nextTask.isDone()) {
                try {
                    nextTask.get(100, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    break;
                } catch (Exception e) {
                    if (debug) {
                        System.err.println("[ParallelExtractor] Error waiting for task: " + e.getMessage());
                    }
                    orderedTasks.poll();
                    activeTasks.remove(nextTask);
                    continue;
                }
            }
            
            orderedTasks.poll();
            activeTasks.remove(nextTask);
            
            try {
                EmbeddedResult result = nextTask.get();
                
                if (handler != null && result.content != null && !result.content.isEmpty()) {
                    handler.characters(result.content.toCharArray(), 0, result.content.length());
                    
                    if (debug) {
                        System.out.println("[ParallelExtractor] ✓ Wrote #" + result.id + 
                            " (" + result.resourceName + ") - " + result.content.length() + " chars IN ORDER");
                    }
                }
            } catch (Exception e) {
                if (debug) {
                    System.err.println("[ParallelExtractor] Error getting result: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Process embedded document (runs in separate thread).
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
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            TikaInputStream tikaStream = TikaInputStream.get(inputStream);
            BodyContentHandler contentHandler = new BodyContentHandler(-1);
            ParseContext context = new ParseContext();
            
            Parser parserToUse = this.parser;
            if (parserToUse == null) {
                parserToUse = new org.apache.tika.parser.AutoDetectParser();
            }
            context.set(Parser.class, parserToUse);
            
            parserToUse.parse(tikaStream, contentHandler, metadata, context);
            
            result.content = contentHandler.toString();
            result.success = true;
            
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
     * Wait for some tasks to complete.
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
     * Wait for all tasks and write results in order.
     */
    public List<EmbeddedResult> waitForAllTasks(ContentHandler handler) throws SAXException {
        List<EmbeddedResult> results = new ArrayList<>();
        
        if (debug) {
            System.out.println("[ParallelExtractor] Waiting for remaining " + orderedTasks.size() + " tasks...");
        }
        
        while (!orderedTasks.isEmpty()) {
            Future<EmbeddedResult> task = orderedTasks.poll();
            if (task != null) {
                try {
                    EmbeddedResult result = task.get(timeoutSeconds, TimeUnit.SECONDS);
                    results.add(result);
                    
                    if (handler != null && result.content != null && !result.content.isEmpty()) {
                        handler.characters(result.content.toCharArray(), 0, result.content.length());
                        
                        if (debug) {
                            System.out.println("[ParallelExtractor] ✓ Final write #" + result.id + " IN ORDER");
                        }
                    }
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
        }
        
        activeTasks.clear();
        
        if (debug) {
            System.out.println("[ParallelExtractor] All tasks completed. Results: " + results.size());
        }
        
        return results;
    }
    
    /**
     * Shutdown the executor service.
     */
    public void shutdown(ContentHandler handler) throws SAXException {
        waitForAllTasks(handler);
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
        String value = System.getProperty(key);
        if (value == null) {
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
     * Result of processing an embedded document.
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
 * Factory for creating ParallelEmbeddedDocumentExtractor instances.
 * Used by Tika via tika-config.xml configuration.
 */
public class ParallelEmbeddedDocumentExtractorFactory implements EmbeddedDocumentExtractorFactory {
    
    @Override
    public EmbeddedDocumentExtractor newInstance(Metadata metadata, ParseContext context) {
        return new ParallelEmbeddedDocumentExtractor(context);
    }
}
