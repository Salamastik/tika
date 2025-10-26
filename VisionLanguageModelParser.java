package org.apache.tika.parser.vision;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.net.ProxySelector;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateFactory;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.io.ByteArrayInputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Custom Tika Parser that integrates with Vision Language Models
 * Supports OpenAI GPT-4 Vision, Anthropic Claude, and custom VLM endpoints
 * 
 * <h3>Logging:</h3>
 * This parser uses SLF4J logger. To see DEBUG logs:
 * <ul>
 * <li>Configure your logging framework (logback, log4j2, etc.)</li>
 * <li>Set logger level to DEBUG for: org.apache.tika.parser.vision.VisionLanguageModelParser</li>
 * </ul>
 * 
 * Example logback.xml:
 * <pre>
 * &lt;logger name="org.apache.tika.parser.vision" level="DEBUG"/&gt;
 * </pre>
 * 
 * By default, only WARN and ERROR logs are shown.
 */
public class VisionLanguageModelParser extends AbstractParser {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(VisionLanguageModelParser.class);
    
    // Configuration properties
    private String apiEndpoint;
    private String apiKey;
    private String modelName;
    private String provider;
    private String prompt;
    private int maxImageSize = 20 * 1024 * 1024;
    private int timeout = 30;
    private String customCertificate;
    
    private static final String DEFAULT_PROMPT = "Please analyze this image and provide a detailed description " +
                                               "including: 1) Main subjects and objects, 2) Text content if any, " +
                                               "3) Scene/setting, 4) Colors and composition, 5) Any notable details. " +
                                               "Format the response as structured text.";
    
    private static final Set<MediaType> SUPPORTED_TYPES = 
        Collections.unmodifiableSet(new HashSet<MediaType>() {{
            add(MediaType.image("jpeg"));
            add(MediaType.image("jpg"));
            add(MediaType.image("png"));
            add(MediaType.image("gif"));
            add(MediaType.image("bmp"));
            add(MediaType.image("webp"));
            add(MediaType.image("tiff"));
        }});
    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpClient httpClient;
    private HttpClient unsafeHttpClient;

    public VisionLanguageModelParser() {
        this.provider = System.getProperty("tika.vlm.provider", 
                       System.getenv("TIKA_VLM_PROVIDER") != null ? 
                       System.getenv("TIKA_VLM_PROVIDER") : "openai");
        
        this.apiKey = System.getProperty("tika.vlm.apikey", 
                     System.getenv("TIKA_VLM_API_KEY"));
        
        this.modelName = System.getProperty("tika.vlm.model", 
                        System.getenv("TIKA_VLM_MODEL") != null ? 
                        System.getenv("TIKA_VLM_MODEL") : "gpt-4-vision-preview");
        
        this.prompt = System.getProperty("tika.vlm.prompt", 
                     System.getenv("TIKA_VLM_PROMPT") != null ? 
                     System.getenv("TIKA_VLM_PROMPT") : DEFAULT_PROMPT);
        
        this.customCertificate = System.getProperty("tika.vlm.certificate",
                                System.getenv("TIKA_VLM_CERTIFICATE"));
        
        if ("openai".equalsIgnoreCase(provider)) {
            this.apiEndpoint = System.getProperty("tika.vlm.endpoint", 
                              "https://api.openai.com/v1/chat/completions");
        } else if ("anthropic".equalsIgnoreCase(provider)) {
            this.apiEndpoint = System.getProperty("tika.vlm.endpoint", 
                              "                                     ");
        } else {
            this.apiEndpoint = System.getProperty("tika.vlm.endpoint", 
                              System.getenv("TIKA_VLM_ENDPOINT"));
        }
        
        String timeoutStr = System.getProperty("tika.vlm.timeout", 
                           System.getenv("TIKA_VLM_TIMEOUT"));
        if (timeoutStr != null) {
            try {
                this.timeout = Integer.parseInt(timeoutStr);
            } catch (NumberFormatException e) {
                // Keep default
            }
        }
        this.httpClient = createHttpClient();
    }

    private HttpClient createHttpClient() {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_2);
            
            String proxyHost = System.getProperty("https.proxyHost");
            String proxyPort = System.getProperty("https.proxyPort");
            if (proxyHost != null && proxyPort != null) {
                builder.proxy(ProxySelector.of(
                    new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort))
                ));
            }
            
            if (customCertificate != null && !customCertificate.trim().isEmpty()) {
                SSLContext sslContext = createSSLContextWithCustomCert(customCertificate);
                builder.sslContext(sslContext);
                LOGGER.info("Loaded custom certificate from configuration");
            } else {
                // Configure SSL Context for better compatibility
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, null, new SecureRandom());
                builder.sslContext(sslContext);
            }
            
            return builder.build();
            
        } catch (Exception e) {
            LOGGER.warn("Failed to configure SSL context: {}", e.getMessage(), e);
            return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        }
    }

    /**
     * Create SSL context with custom certificate from string.
     * 
     * @param certContent Certificate content in PEM format
     * @return Configured SSLContext
     */
    private SSLContext createSSLContextWithCustomCert(String certContent) throws Exception {
        certContent = certContent.trim();
        
        if (!certContent.startsWith("-----BEGIN CERTIFICATE-----")) {
            certContent = "-----BEGIN CERTIFICATE-----\n" + 
                         certContent + 
                         "\n-----END CERTIFICATE-----";
        }
        
        byte[] certBytes = certContent.getBytes("UTF-8");
        
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate cert = (X509Certificate) cf.generateCertificate(
            new ByteArrayInputStream(certBytes)
        );
        
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("custom-cert", cert);
        
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm()
        );
        tmf.init(keyStore);
        
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
        
        return sslContext;
    }

    private HttpClient getUnsafeHttpClient() {
        if (unsafeHttpClient == null) {
            unsafeHttpClient = UnsafeHttpClient.createUnsafeClient();
        }
        return unsafeHttpClient;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler,
                     Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        
        if (apiKey == null || apiEndpoint == null) {
            throw new TikaException("VLM API not configured. Set TIKA_VLM_API_KEY and TIKA_VLM_ENDPOINT");
        }

        byte[] imageData = readInputStream(stream);    
        if (imageData.length > maxImageSize) {
            throw new TikaException("Image size exceeds maximum allowed size of " + maxImageSize + " bytes");
        }

        String base64Image = Base64.getEncoder().encodeToString(imageData);
        
        String mimeType = metadata.get(Metadata.CONTENT_TYPE);
        if (mimeType == null) {
            mimeType = "image/jpeg";
        }

        try {
            LOGGER.info("Starting VLM analysis for image");
            // String analysis = callVisionAPI(base64Image, mimeType);
            String analysis = "HII";
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            
            xhtml.startElement("div", "class", "vlm-analysis");
            xhtml.startElement("h2");
            xhtml.characters("Vision Language Model Analysis");
            xhtml.endElement("h2");
            
            xhtml.startElement("p");
            xhtml.characters(analysis);
            xhtml.endElement("p");

            // Use colon-separated metadata keys to match the injector expectations
            metadata.add("vlm:provider", provider);
            metadata.add("vlm:model", modelName);
            metadata.add("vlm:prompt", prompt);
            metadata.add("vlm:analysis", analysis);
                        
            xhtml.endElement("div");
            xhtml.endDocument();
            LOGGER.info("Completed VLM analysis for image");
        } catch (Exception e) {
            throw new TikaException("Failed to analyze image with VLM", e);
        }
    }

    private byte[] readInputStream(InputStream stream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = stream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toByteArray();
    }

    private String callVisionAPI(String base64Image, String mimeType) throws Exception {
        HttpRequest request;
        String requestBody;
        
        try {
            if ("openai".equalsIgnoreCase(provider)) {
                requestBody = buildOpenAIRequest(base64Image, mimeType);
                request = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .timeout(Duration.ofSeconds(timeout))
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
                    
            } else if ("anthropic".equalsIgnoreCase(provider)) {
                requestBody = buildAnthropicRequest(base64Image, mimeType);
                request = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint))
                    .header("Content-Type", "application/json")
                    .header("x-api-key", apiKey)
                    .header("anthropic-version", "2023-06-01")
                    .timeout(Duration.ofSeconds(timeout))
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
                    
            } else {
                requestBody = buildOpenAIRequest(base64Image, mimeType);
                request = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .timeout(Duration.ofSeconds(timeout))
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            }

            HttpResponse<String> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                LOGGER.info("Successfully connected with secure HTTP client");
            } catch (Exception e) {
                LOGGER.info("Secure HTTP client failed: {}", e.getMessage());
                LOGGER.info("Falling back to unsafe HTTP client...");
                
                try {
                    response = getUnsafeHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
                    LOGGER.info("Successfully connected with unsafe HTTP client");
                } catch (Exception unsafeException) {
                    String errorMsg = "Both secure and unsafe HTTP clients failed:\n";
                    errorMsg += "Secure client error: " + e.getMessage() + "\n";
                    errorMsg += "Unsafe client error: " + unsafeException.getMessage() + "\n";
                    
                    if (e instanceof javax.net.ssl.SSLHandshakeException) {
                        errorMsg += "\nSSL Handshake failed when connecting to " + apiEndpoint + ". ";
                        errorMsg += "Possible causes: \n";
                        errorMsg += "1. Behind a corporate proxy (set -Dhttps.proxyHost and -Dhttps.proxyPort)\n";
                        errorMsg += "2. Outdated Java version (requires Java 11+)\n";
                        errorMsg += "3. Missing CA certificates in trust store\n";
                        errorMsg += "4. Network firewall blocking HTTPS traffic\n";
                    }
                    
                    throw new TikaException(errorMsg, e);
                }
            }
            
            if (response.statusCode() != 200) {
                throw new IOException("VLM API returned status " + response.statusCode() + 
                                    ": " + response.body());
            }
    
            return parseAPIResponse(response.body());
            
        } catch (TikaException e) {
            throw e;
        } catch (Exception e) {
            throw new TikaException("Failed to call VLM API: " + e.getMessage(), e);
        }
    }

    private String buildOpenAIRequest(String base64Image, String mimeType) 
            throws Exception {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("model", modelName);
        
        ArrayNode messages = root.putArray("messages");
        ObjectNode message = messages.addObject();
        message.put("role", "user");
        
        ArrayNode content = message.putArray("content");
        
        ObjectNode textContent = content.addObject();
        textContent.put("type", "text");
        textContent.put("text", prompt);
        
        ObjectNode imageContent = content.addObject();
        imageContent.put("type", "image_url");
        ObjectNode imageUrl = imageContent.putObject("image_url");
        imageUrl.put("url", "data:" + mimeType + ";base64," + base64Image);
        
        root.put("max_tokens", 1000);
        root.put("temperature", 0.5);
        
        return objectMapper.writeValueAsString(root);
    }

    private String buildAnthropicRequest(String base64Image, String mimeType) 
            throws Exception {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("model", modelName);
        
        ArrayNode messages = root.putArray("messages");
        ObjectNode message = messages.addObject();
        message.put("role", "user");
        
        ArrayNode content = message.putArray("content");
        
        ObjectNode imageContent = content.addObject();
        imageContent.put("type", "image");
        ObjectNode source = imageContent.putObject("source");
        source.put("type", "base64");
        source.put("media_type", mimeType);
        source.put("data", base64Image);
        
        ObjectNode textContent = content.addObject();
        textContent.put("type", "text");
        textContent.put("text", prompt);
        
        root.put("max_tokens", 1000);
        
        return objectMapper.writeValueAsString(root);
    }

    private String parseAPIResponse(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        
        if ("openai".equalsIgnoreCase(provider)) {
            JsonNode choices = root.get("choices");
            if (choices != null && choices.size() > 0) {
                JsonNode message = choices.get(0).get("message");
                if (message != null) {
                    JsonNode content = message.get("content");
                    if (content != null) {
                        return content.asText();
                    }
                }
            }
        } else if ("anthropic".equalsIgnoreCase(provider)) {
            JsonNode content = root.get("content");
            if (content != null && content.size() > 0) {
                JsonNode text = content.get(0).get("text");
                if (text != null) {
                    return text.asText();
                }
            }
        } else {
            String result = null;
            try {
                JsonNode choices = root.get("choices");
                if (choices != null && choices.size() > 0) {
                    result = choices.get(0).get("message").get("content").asText();
                }
            } catch (Exception e) {
                JsonNode content = root.get("content");
                if (content != null && content.size() > 0) {
                    result = content.get(0).get("text").asText();
                }
            }
            if (result != null) return result;
        }
        
        throw new IOException("Unable to parse VLM API response");
    }

}

class UnsafeHttpClient {
    public static HttpClient createUnsafeClient() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
            };
            
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            
            return HttpClient.newBuilder()
                .sslContext(sslContext)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
                
        } catch (Exception e) {
            throw new RuntimeException("Failed to create unsafe HTTP client", e);
        }
    }
}
