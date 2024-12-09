import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// Sample Java program to read files from hadoop hdfs filesystem
public class HDFSDemo {

    // This is copied from the entry in core-site.xml for the property fs.defaultFS.
    // Replace with your Hadoop deployment details.
    public static final String HDFS_ROOT_URL="hdfs://master:9000";
    private Configuration conf;



    public static void main(String[] args) throws Exception {
        HDFSDemo demo = new HDFSDemo();

        // Reads a file from the user's home directory.
        // Replace jj with the name of your folder
        // Assumes that input.txt is already in HDFS folder

        String uri = HDFS_ROOT_URL+"/user/temp/4300-0.txt";
        demo.printHDFSFileContents(uri);
    }

    public HDFSDemo() {
        conf = new Configuration();
    }

    // Example - Print hdfs file contents to console using Java
    public void printHDFSFileContents(String uri) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

}