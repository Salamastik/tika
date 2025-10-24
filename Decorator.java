// ParallelizingParserDecorator.java
package org.apache.tika.parallel;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Decorator that installs ParallelEmbeddedDocumentExtractorFactory
 * and performs placeholder replacement at the end of parsing.
 * 
 * WORKFLOW:
 * 1. Parser finds embedded documents (images) → calls parseEmbedded()
 * 2. Factory writes placeholders ({{VLM_PLACEHOLDER_*}}) immediately → no blocking
 * 3. Factory spawns async VLM tasks in background
 * 4. Parser continues streaming content → all goes to buffer
 * 5. Parser reaches endDocument() → triggers performReplacement()
 * 6. performReplacement() calls awaitAll() → waits for all VLM tasks
 * 7. Gets replacements map → replaces all placeholders → writes final output
 * 
 * KEY POINT: The decorator MUST call awaitAll() before getReplacements()!
 */
public class ParallelizingParserDecorator extends ParserDecorator {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ParallelizingParserDecorator.class);

    public ParallelizingParserDecorator() {
        super(new AutoDetectParser(TikaConfig.getDefaultConfig()));
        LOGGER.info("[Decorator] Initialized with AutoDetectParser");
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler,
                      Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {

        // Set the parallel factory
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractorFactory.class,
                new ParallelEmbeddedDocumentExtractorFactory());
        LOGGER.info("[Decorator] parse() started with ParallelEmbeddedDocumentExtractorFactory");

        // Create a buffering handler that captures all output
        PlaceholderReplacingHandler replacingHandler = new PlaceholderReplacingHandler(handler);

        // Run the parsing with our buffering handler
        Parser wrapped = getWrappedParser();
        wrapped.parse(stream, replacingHandler, metadata, context);

        LOGGER.info("[Decorator] parse() finished, performing placeholder replacement");
    }

    /**
     * ContentHandler that buffers all output and performs placeholder replacement at the end.
     */
    private static class PlaceholderReplacingHandler extends ContentHandlerDecorator {
        
        private static final Logger HANDLER_LOGGER = 
            LoggerFactory.getLogger(PlaceholderReplacingHandler.class);
        
        private final StringBuilder buffer = new StringBuilder();
        private boolean replacementDone = false;

        public PlaceholderReplacingHandler(ContentHandler delegate) {
            super(delegate);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            // Buffer the characters instead of writing immediately
            buffer.append(ch, start, length);
        }

        @Override
        public void endDocument() throws SAXException {
            if (!replacementDone) {
                performReplacement();
                replacementDone = true;
            }
            super.endDocument();
        }

        private void performReplacement() throws SAXException {
            HANDLER_LOGGER.info("Starting placeholder replacement (buffer size: {} chars)", 
                buffer.length());

            // CRITICAL: Wait for all embeds to complete FIRST!
            ParallelEmbeddedDocumentExtractorFactory.awaitAll();

            // Get all replacements from the factory
            Map<String, String> replacements = 
                ParallelEmbeddedDocumentExtractorFactory.getReplacements();

            if (replacements.isEmpty()) {
                HANDLER_LOGGER.info("No replacements needed");
                // Just write the buffer as-is
                writeToDelegate(buffer.toString());
                return;
            }

            HANDLER_LOGGER.info("Replacing {} placeholders", replacements.size());

            // Use StringBuilder for efficient replacement
            String result = buffer.toString();
            int replacementCount = 0;
            
            // Option A: Simple replacement (works if placeholders are unique)
            for (Map.Entry<String, String> entry : replacements.entrySet()) {
                String placeholder = entry.getKey();
                String replacement = entry.getValue();
                
                if (result.contains(placeholder)) {
                    result = result.replace(placeholder, replacement);
                    replacementCount++;
                    HANDLER_LOGGER.debug("Replaced placeholder: {}", placeholder);
                }
            }

            HANDLER_LOGGER.info("Replaced {} placeholders in output", replacementCount);

            // Write the final result to the delegate handler
            writeToDelegate(result);
        }

        private void writeToDelegate(String text) throws SAXException {
            if (text == null || text.isEmpty()) {
                return;
            }
            char[] chars = text.toCharArray();
            super.characters(chars, 0, chars.length);
        }
    }
}
