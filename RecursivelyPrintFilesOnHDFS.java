import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 *
 * @author Shashwat Shriparv
 * @email  dwivedishashwat@gmail.com
 * @web    helpmetocode.blogspot.com
 */
public class RecursivelyPrintFilesOnHDFS {

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        printFilesRecursively("hdfs://localhost:9000/user/acadgild/hadoop");
    }

    public static void printFilesRecursively(String Url) throws IOException {
        try {




            FileSystem fs = new DistributedFileSystem();

            fs.initialize(new URI(Url), new Configuration());
            FileStatus[] status = fs.listStatus(new Path(Url));
            for (int i = 0; i < status.length; i++) {
                if (status[i].isDir()) {
                    printFilesRecursively(status[i].getPath().toString());
                } else {
                    try {
                        System.out.println(status[i].getPath().toString());
                    } catch (Exception e) {
                        System.err.println(e.toString());
                    }

                }

            }
        } catch (URISyntaxException ex) {
            Logger.getLogger(RecursivelyPrintFilesOnHDFS.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}

