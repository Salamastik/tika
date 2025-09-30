#!/bin/bash
# Simple script to download all dependencies for offline use

# Resolve dependencies
mvn dependency:resolve

# Download all plugins and dependencies for offline use
mvn dependency:go-offline

echo "All dependencies downloaded. You can now build offline with 'mvn -o package'."

mvn -o clean package

~/.m2/repository