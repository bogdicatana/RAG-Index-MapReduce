### This was a homework for the Cloud Computing class at UIC with [Dr. Mark Grechanik](https://www.cs.uic.edu/~drmark/)

### Video of aws demo: https://youtu.be/xv1wp4rYpQM

## RAG Indexing and Querying with MapReduce

This project implements a **Retrieval-Augmented Generation (RAG)** pipeline using **Hadoop MapReduce** to build an index from a corpus of PDF documents. The index is built using **Apache Lucene**, and the RAG process is completed by querying the index and using the retrieved context to summarize an answer with an **Ollama** large language model (LLM). The entire pipeline is written in **Scala**.

---

## 1. Architecture Overview

The pipeline operates in four main stages:

1.  **Preparation (MapReduce Input):** A list of input PDF file paths is provided. This list serves as the input to the MapReduce job.
2.  **Indexing (MapReduce Job):**
    * **Mapper (`RagMapper.scala`):** Processes individual PDF documents, extracts text, chunks the text, generates vector embeddings for each chunk using **Ollama**, and emits the data to the Reducer, partitioned by document hash.
    * **Reducer (`ShardReducer.scala`):** Receives chunks for its assigned partition, builds a local **Lucene index shard** using the text and vector embeddings, and calculates statistics (vocabulary, token vectors, word relations and analogies). The resulting index and statistics are saved to HDFS.
3.  **Post-Processing & Querying (`Main.scala`):** After the MapReduce job, the main application merges the statistics from all shards. It then loads all Lucene index shards into a single `MultiReader` for a global search.
4.  **RAG (Query & Summarization):** The merged Lucene index is queried with a user-defined text query that is in the configuration file. The top-scoring text chunks are retrieved and used as context in a prompt to the **Ollama** chat model for final summarization.

---

## 2. Implementation Details

### A. Data Processing and Chunking

* **PDF Processing (`PdfProcessor.scala`):** Uses **Apache PDFBox** to extract plain text from PDF files.
* **Chunking (`PdfProcessor.scala`, `RagMapper.scala`):** The extracted text is split into overlapping chunks to maintain context continuity. The chunk size (`max-chars`) and `overlap` are configured in `application.conf` and accessed via `Settings.scala`.

### B. Embedding Generation

* **Ollama Client (`Ollama.scala`):** Implements an HTTP client using the **sttp client3** library to interact with a self-hosted **Ollama** instance (it is assumed that the user runs their own ollama instance locally before running the project).
* The `embed` method is called within the `RagMapper` to generate a float vector embedding for every text chunk using the configured `embedding-model` (please make sure that you have pulled the embedding model you want to use with ollama beforehand).
* The `chat` method is used in `Main.scala` to send the final RAG prompt to the LLM using a `query` and `model` that are defined in the configuration file.

### C. MapReduce Indexing (`RagMapper.scala` & `ShardReducer.scala`)

* **Mapper (`RagMapper.scala`):**
    * Reads a PDF path from the input.
    * Generates chunks and embeddings using `PdfProcessor.scala` and `Ollama.scala`.
    * Calculates a `shard` ID by hashing the document ID (`docId.hashCode % numReducers`) to ensure all chunks from a single document go to the same reducer, or at least are partitioned deterministically.
    * Emits the chunk data (including embeddings) as a JSON string (`Text` value) with the `shard` ID as the key (`IntWritable`).
* **Reducer (`ShardReducer.scala`):**
    * **Lucene Indexing:** It opens a local **Lucene `IndexWriter`** (`FSDirectory` to a temp directory). It iterates through all received chunks, parses the JSON, and creates a Lucene `Document` for each.
        * The vector embedding is stored in a `KnnFloatVectorField` with `VectorSimilarityFunction.COSINE` for efficient vector search.
        * The text is stored in a `TextField` for standard keyword search.
    * **Statistics Generation:** It calculates three types of statistics:
        1.  **Vocabulary:** Token frequencies across the shard's documents, written to `vocab_shard_*.csv`.
        2.  **Approximate Token Embeddings:** Calculates a simple average of the chunk embeddings where a token occurs, providing an approximate token vector representation. These are not stored anywhere but are instead used to compute the `word relation` statistics.
        3.  **Word Relations:** Uses the approximate token vectors to find nearest neighbors, compute similarities between pre-defined pairs, and solve analogies (written to `neighbors_shard_*.csv`, `similarities_shard_*.csv`, and `analogies_shard_*.csv` respectively).
    * **Cleanup:** The local Lucene index directory and statistics files are copied to the HDFS output path using the `copyLocalToHdfs` helper.

### D. Main Application Flow (`Main.scala`)

* **Environment Setup:** Sets up environment based on input/output paths and sets the Hadoop configuration accordingly.
* **MapReduce Execution:** Runs the `rag-index-builder` job, using `NLineInputFormat` to control the number of PDF paths processed per mapper split, with the max number of mappers being the same as the max number of reducers (with both defined in the configuration file).
* **Stats Merging:** The `mergeStats` helper function aggregates the per-shard CSV files. Vocabulary is summed, while other files (neighbors, similarities, analogies) are simply concatenated. These will be present in the same output directory as the shards under `merged_stats`.
* **Index Loading:** Loads all `index_shard_*` directories as local Lucene index directories and combines them into a single `MultiReader` for unified searching.
* **Query and RAG:**
    * Performs a standard Lucene keyword search using `QueryParser` and retrieves the top-N results (N can be set in the configuration file).
    * The text from the top results is concatenated into a `contextText`.
    * A structured prompt, including the system role, context, and user question, is constructed and sent to the **Ollama** chat endpoint for the final answer.

---

## 3. Configuration

The project's behavior is controlled by the settings in `application.conf`.

| Setting                   | File/Code Usage              | Description                                                                                                |
|:--------------------------|:-----------------------------|:-----------------------------------------------------------------------------------------------------------|
| `ollama.host`             | `Ollama.scala`, `Main.scala` | URL of the Ollama server. Can be overridden by the `OLLAMA_HOST` environment variable.                     |
| `ollama.embedding-model`  | `RagMapper.scala`            | The Ollama model used to generate chunk vectors (e.g., `mxbai-embed-large`).                               |
| `ollama.chat-model`       | `Main.scala`                 | The Ollama model used for the final RAG summarization (e.g., `llama3.1:8b`).                               |
| `ollama.query`            | `Main.scala`                 | The text query to search the index with.                                                                   |
| `ollama.timeout`          | `Ollama.scala`               | The amount of time to wait before terminating a request to Ollama.                                         |
| `ollama.topResults`       | `Main.scala`                 | How many chunks do you want the context to receive in the query.                                           |
| `chunker.max-chars`       | `RagMapper.scala`            | Maximum number of words in a text chunk.                                                                   |
| `chunker.overlap`         | `RagMapper.scala`            | Number of words to overlap between sequential chunks.                                                      |
| `mapReduce.numReduceJobs` | `Main.scala`                 | Number of reducers, which determines the number of index shards created.                                   |
| `mapReduce.numMappers`    | `Main.scala`                 | Number of maximum mappers to be created. The actual amount is generated by how many input pdfs you have.   |
| `input.pdfs`              | `Main.scala`                 | Path (local or HDFS) to the file containing the list of PDF paths to index (usually a `.txt`).             |
| `input.pdfsPerSplit`      | `Main.scala`                 | How many pdfs from the input to assign to each split (each is a `mapper` up to the max number of mappers). |
| `output.outputDir`        | `Main.scala`                 | Output directory for the index shards and statistics.                                                      |
| `word-relations.*`        | `ShardReducer.scala`         | Lists of word pairs/triplets for which similarity and analogy statistics are calculated.                   |

## 4. Setup and deployment

### A. On user's computer

You can change the settings in the configuration file and add more pdf paths to the `input_pdfs.txt` file if you want or use the `allpdfs.txt` file to create an index of all the pdfs.

This project can be ran using the following commands (assuming you are in the project directory):

```bash
sbt clean compile run // to run the whole job
sbt clean compile test // to run the tests
```


### B. On AWS EC2 computer

First you will need an `AWS` account and ideally `aws cli`. This is not going to be explained here as it is not unique to this specific project and there is documentation online for them.

Now you'll have to create an `EC2` instance in `AWS` and download the key pair `.pem` file.
Go to **EC2 -> Launch Instance** and select ubuntu and a key pair for login. Then assign at least 30 GB of storage to the instance and launch.

You can now ssh into it with the `.pem` file:

```bash
ssh -i path/to/pem ubuntu@<your ec2 instance>.compute-1.amazonaws.com
```

You will want to send the pdfs here as well since having them "locally" on the `EC2` instance will improve performance.

```bash
scp -i path/to/pem path/to/MSRCorpus ubuntu@<your ec2 dns>:~/input
scp -i path/to/pem path/to/input_pdfs_aws.txt ubuntu@<your ec2 dns>:~
```

Ok now let's set up `Ollama` and `Hadoop`:

```bash
sudo apt update && sudo apt install -y openjdk-21-jdk unzip wget // install java and unzip, wget

// Setup hadoop
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
tar -xzf hadoop-3.4.2.tar.gz
sudo mv hadoop-3.4.2 /usr/local/hadoop
echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
hadoop version // to test that it installed correctly

// Setup Ollama

curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3.1:8b
ollama pull mxbai-embed-large
sudo systemctl start ollama // to start ollama server in the background
curl http://127.0.0.1:11434/api/tags // to test that everything is running and installed
```

Next we will go to our project on our machine and run *(change `input_pdfs.txt` to `input_pdfs_aws.txt` in the config and uncomment the "provided" hadoop dependencies and comment the regular ones in `build.sbt` first!!!)*:

```bash
sbt clean assembly
scp -i path/to/pem target/scala-3.4.0/cs441hw1-assembly-0.1.0-SNAPSHOT.jar ubuntu@<your ec2 dns>:~
```

We can now go back to the the `EC2` instance and run to rag the llm and query it:

```bash
hadoop jar cs441hw1-assembly-0.1.0-SNAPSHOT.jar
```

You can now copy the output directory back to your personal computer if you want. ***Remember to shut down the `EC2` instance so you don't lose all your money.***
