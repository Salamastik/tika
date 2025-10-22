#!/bin/bash
# Simple script to download all dependencies for offline use

# Resolve dependencies
mvn dependency:resolve

# Download all plugins and dependencies for offline use
mvn dependency:go-offline

echo "All dependencies downloaded. You can now build offline with 'mvn -o package'."

mvn -o clean package

~/.m2/repository










 docker run -p 9998:9998 -v $(pwd)/config.xml:/config.xml -e TIKA_VLM_ENDPOINT='g' -e TIKA_VLM_API_KEY='h' g:g12 -c /config.xml
