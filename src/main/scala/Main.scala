import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import rag.*

object Main {
    def main(args: Array[String]): Unit = {

        val conf = new Configuration(false) // start with empty config, ignore system XMLs
        conf.set("mapreduce.framework.name", "local")
        conf.set("fs.defaultFS", "file:///")
        val job = Job.getInstance(conf, "rag-index-builder")
        job.setJarByClass(classOf[RagMapper])
        job.setMapperClass(classOf[RagMapper])
        job.setReducerClass(classOf[ShardReducer])

        job.setMapOutputKeyClass(classOf[IntWritable])
        job.setMapOutputValueClass(classOf[Text])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        job.setNumReduceTasks(4)

        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))

        System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
}