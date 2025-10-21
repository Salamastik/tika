// ParallelEmbeddedDocumentExtractorFactory.java
package org.apache.tika.parallel;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentExtractorFactory;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.config.TikaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.AttributesImpl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 * Parallel EmbeddedDocumentExtractorFactory with PLACEHOLDER injection.
 * Writes placeholders immediately, replaces them at the end with actual results.
 */
public class ParallelEmbeddedDocumentExtractorFactory implements EmbeddedDocumentExtractorFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ParallelEmbeddedDocumentExtractorFactory.class);

    // Shared executor
    private static final int CONCURRENCY = Integer.parseInt(
            System.getProperty("tika.vlm.threads",
                    System.getenv().getOrDefault("TIKA_VLM_THREADS", "6"))
    );

    private static final ExecutorService EXEC = new ThreadPoolExecutor(
            CONCURRENCY, CONCURRENCY,
            30L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            r -> {
                Thread t = new Thread(r, "vlm-worker-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("[Factory] Shutting down executor");
            EXEC.shutdown();
            try {
                if (!EXEC.awaitTermination(10, TimeUnit.SECONDS)) {
                    EXEC.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private static final long DEFAULT_MAX_EMBED_BYTES = Long.parseLong(
            System.getProperty("tika.vlm.maxEmbedBytes",
                    System.getenv().getOrDefault("TIKA_VLM_MAX_EMBED_BYTES", "10485760"))
    );
    
    private static final long DEFAULT_WAIT_ALL_MS = Long.parseLong(
            System.getProperty("tika.vlm.waitAllMs",
                    System.getenv().getOrDefault("TIKA_VLM_WAIT_ALL_MS", "60000")) // 60s for all
    );

    // ===== GLOBAL STATE (ThreadLocal for thread-safety) =====
    private static final ThreadLocal<Map<String, PendingEmbed>> REQUEST_EMBEDS =
            ThreadLocal.withInitial(LinkedHashMap::new); // LinkedHashMap keeps insertion order

    @Override
    public EmbeddedDocumentExtractor newInstance(Metadata parentMd, ParseContext context) {
        final Parser embeddedParserTemplate = Objects.requireNonNullElseGet(
                context.get(Parser.class), AutoDetectParser::new);

        final ParsingEmbeddedDocumentExtractor delegate =
                new ParsingEmbeddedDocumentExtractor(context);

        LOGGER.info("[Factory] newInstance created");

        return new EmbeddedDocumentExtractor() {
            @Override
            public boolean shouldParseEmbedded(Metadata metadata) {
                boolean should = delegate.shouldParseEmbedded(metadata);
                if (!should) {
                    LOGGER.debug("[Factory] shouldParseEmbedded=false for {}", metadata.get("resourceName"));
                }
                return should;
            }

            @Override
            public void parseEmbedded(InputStream stream,
                                      ContentHandler handler,
                                      Metadata metadata,
                                      boolean outputHtml) throws SAXException, IOException {
                
                // Read into memory
                final byte[] data;
                try {
                    data = stream.readAllBytes();
                } catch (IOException e) {
                    LOGGER.warn("[Factory] Failed to read {}", metadata.get("resourceName"), e);
                    return;
                }

                if (data.length > DEFAULT_MAX_EMBED_BYTES) {
                    LOGGER.warn("[Factory] Embed too large ({} bytes) for {}", 
                        data.length, metadata.get("resourceName"));
                    return;
                }

                final Metadata mdCopy = copyMetadata(metadata);
                final String path = normalizePath(mdCopy);
                final String placeholder = "{{VLM_PLACEHOLDER_" + sanitizePath(path) + "}}";

                // 1. Write placeholder IMMEDIATELY (no blocking!)
                safeWriteText(handler, "\n" + placeholder + "\n");
                LOGGER.info("[Factory] Wrote placeholder for {}", path);

                // 2. Schedule async parsing
                CompletableFuture<Metadata> fut = CompletableFuture.supplyAsync(() -> {
                    LOGGER.info("[Factory] Processing {} (thread={})", path, Thread.currentThread().getName());
                    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
                        Parser p;
                        try {
                            p = new AutoDetectParser(TikaConfig.getDefaultConfig());
                        } catch (Exception ex) {
                            p = new AutoDetectParser();
                        }
                        ParseContext ctxForTask = new ParseContext();
                        try {
                            p.parse(bais, new DefaultHandler(), mdCopy, ctxForTask);
                        } catch (Exception e) {
                            mdCopy.add("vlm:error", "parse-failed:" + e.getClass().getSimpleName());
                            LOGGER.warn("[Factory] Parse error for {} – {}", path, e.toString());
                        }
                    } catch (Exception e) {
                        mdCopy.add("vlm:error", "stream-failed");
                        LOGGER.warn("[Factory] Stream error for {} – {}", path, e.toString());
                    }
                    LOGGER.info("[Factory] Completed {} (vlm:analysis present: {})", 
                        path, mdCopy.get("vlm:analysis") != null);
                    return mdCopy;
                }, EXEC);

                // 3. Store for later replacement
                REQUEST_EMBEDS.get().put(path, new PendingEmbed(placeholder, fut, path));
                
                LOGGER.debug("[Factory] Queued {} for async processing (total: {})", 
                    path, REQUEST_EMBEDS.get().size());
            }
        };
    }

    // ===== PUBLIC STATIC METHODS FOR COORDINATION =====

    /**
     * Wait for all pending embeds to complete (called before getReplacements).
     */
    public static void awaitAll() {
        Map<String, PendingEmbed> embeds = REQUEST_EMBEDS.get();
        
        if (embeds.isEmpty()) {
            LOGGER.info("[Factory] No embeds to await");
            return;
        }

        LOGGER.info("[Factory] Awaiting {} embeds to complete...", embeds.size());
        long startTime = System.currentTimeMillis();

        CompletableFuture<Void> allDone = CompletableFuture.allOf(
            embeds.values().stream()
                .map(pe -> pe.future)
                .toArray(CompletableFuture[]::new)
        );

        try {
            allDone.get(DEFAULT_WAIT_ALL_MS, TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("[Factory] All {} embeds completed in {} ms", embeds.size(), elapsed);
        } catch (TimeoutException e) {
            LOGGER.warn("[Factory] Timeout after {} ms waiting for embeds", DEFAULT_WAIT_ALL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("[Factory] Interrupted while waiting");
        } catch (ExecutionException e) {
            LOGGER.error("[Factory] Execution error while waiting", e);
        }
    }

    /**
     * Returns a map of placeholder -> replacement text for all completed embeds.
     * Must call awaitAll() first!
     */
    public static Map<String, String> getReplacements() {
        Map<String, PendingEmbed> embeds = REQUEST_EMBEDS.get();
        
        if (embeds.isEmpty()) {
            LOGGER.info("[Factory] No embeds to process");
            cleanup();
            return Collections.emptyMap();
        }

        LOGGER.info("[Factory] Building replacements for {} embeds", embeds.size());
        long startTime = System.currentTimeMillis();

        Map<String, String> replacements = new LinkedHashMap<>();

        // Build replacement map (all futures should be done now)
        for (PendingEmbed pe : embeds.values()) {
            try {
                if (pe.future.isDone() && !pe.future.isCompletedExceptionally()) {
                    Metadata result = pe.future.get(100, TimeUnit.MILLISECONDS); // Quick get
                    replacements.put(pe.placeholder, buildReplacementText(pe.path, result));
                } else if (pe.future.isCompletedExceptionally()) {
                    replacements.put(pe.placeholder, buildErrorText(pe.path));
                } else {
                    // Still not done after awaitAll? Mark as timeout
                    replacements.put(pe.placeholder, buildTimeoutText(pe.path));
                }
            } catch (Exception e) {
                LOGGER.warn("[Factory] Failed to get result for {}", pe.path, e);
                replacements.put(pe.placeholder, buildErrorText(pe.path));
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("[Factory] Built {} replacements in {} ms", replacements.size(), elapsed);

        cleanup();
        return replacements;
    }

    // ===== Helper Methods =====

    private static void cleanup() {
        REQUEST_EMBEDS.get().clear();
        REQUEST_EMBEDS.remove();
        LOGGER.debug("[Factory] Cleaned up ThreadLocal state");
    }

    private static String buildReplacementText(String path, Metadata md) {
        StringBuilder sb = new StringBuilder();
        String analysis = md.get("vlm:analysis");
        String provider = md.get("vlm:provider");
        String model = md.get("vlm:model");

        sb.append("\n=== VLM Analysis for ").append(path).append(" ===\n");
        if (analysis != null && !analysis.isEmpty()) {
            sb.append(analysis).append("\n");
        } else {
            sb.append("(no analysis)\n");
        }
        sb.append("[provider=").append(String.valueOf(provider))
          .append(", model=").append(String.valueOf(model))
          .append("]\n");
        
        return sb.toString();
    }

    private static String buildTimeoutText(String path) {
        return "\n=== VLM Analysis TIMEOUT for " + path + " ===\n";
    }

    private static String buildErrorText(String path) {
        return "\n=== VLM Analysis FAILED for " + path + " ===\n";
    }

    private static Metadata copyMetadata(Metadata src) {
        Metadata dst = new Metadata();
        for (String n : src.names()) {
            dst.set(n, src.get(n));
        }
        return dst;
    }

    private static String normalizePath(Metadata md) {
        String path = firstNonNull(
                md.get("X-TIKA:final_embedded_resource_path"),
                md.get("X-TIKA:embedded_resource_path"),
                md.get("resourceName")
        );
        if (path == null) {
            path = "__unknown_" + System.nanoTime();
        }
        if (path.startsWith("embedded:")) {
            path = path.substring("embedded:".length());
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    private static String sanitizePath(String path) {
        // Make path safe for use in placeholder (remove special chars)
        return path.replaceAll("[^a-zA-Z0-9/_-]", "_");
    }

    private static String firstNonNull(String a, String b, String c) {
        return a != null ? a : (b != null ? b : c);
    }

    private static void safeWriteText(ContentHandler h, String s) throws SAXException {
        if (s == null || s.isEmpty()) return;
        writeChars(h, s);
    }

    private static void writeChars(ContentHandler h, String s) throws SAXException {
        if (s == null || s.isEmpty()) return;
        char[] c = s.toCharArray();
        h.characters(c, 0, c.length);
    }

    // ===== Inner Classes =====

    static class PendingEmbed {
        final String placeholder;
        final CompletableFuture<Metadata> future;
        final String path;

        PendingEmbed(String placeholder, CompletableFuture<Metadata> future, String path) {
            this.placeholder = placeholder;
            this.future = future;
            this.path = path;
        }
    }
}
