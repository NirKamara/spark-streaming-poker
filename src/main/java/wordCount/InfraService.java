package wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Component;


@Component
public class InfraService {

    /*
    public JavaSparkContext initSparkContext() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("PokerStreamingApp");
        sparkConf.setMaster("local[2]");

        return new JavaSparkContext(sparkConf);
    }

    public SQLContext initSQLContxt(JavaSparkContext sc) {
        return new SQLContext(sc);
    }

    public JavaStreamingContext initStreamingContext(JavaSparkContext sc) {
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        ssc.checkpoint(".");
        return ssc;
    }
    */
}

