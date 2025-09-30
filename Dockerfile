FROM apache/tika:latest-full
RUN mkdir -p /tika-extras 
COPY tika-vlm-parser-1.0.0.jar /tika-extras