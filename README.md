### Document InvertedIndex builder 

To build inverted index for searching documents from the knowledge base documents repository.


#### Setup

Build the maven

```bash
 $ mvn clean install
```

### Command to run spark application
spark2-submit --master yarn --class com.examples.document.searchindex.IndexGeneratorApp --deploy-mode cluster ~/invertedindex-1.0-jar-with-dependencies.jar [data files directory]  [results output path]

The results will be stored in output path.
