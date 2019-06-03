package maksim.ia;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple3;

public class Main {
    
    private static final String LOG_REGEX = "^(\\S+) (\\S+) (\\S+) "
                                            + "\\[([\\w:/]+\\s[+\\-]\\d{4})] \"(\\S+)"
                                            + " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";
    private static Pattern compile = Pattern.compile(LOG_REGEX);
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Test");
        sparkConf.setMaster("spark://10.0.0.100:7077");
        sparkConf.setJars(new String[]{"target/victoria-jar-with-dependencies.jar"});
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://10.0.0.100:9000/nasa/NASA_access_log_Jul95");
        
        taskOne(stringJavaRDD);
        taskTwo(stringJavaRDD);
        taskThree(stringJavaRDD);
        
    }
    
    // Подготовить список запросов, которые закончились 5xx ошибкой, с количеством неудачных запросов
    private static void taskOne(JavaRDD<String> rdd) {
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rdd.filter(log -> {
            Matcher matcher = compile.matcher(log);
            if (matcher.find()) {
                return matcher.group(8).startsWith("5");
            } else {
                return false;
            }
        }).mapToPair(log -> {
            Matcher matcher = compile.matcher(log);
            if (matcher.find()) {
                return new Tuple2<>(matcher.group(5) + " " + matcher.group(6), 1);
            } else {
                return null;
            }
        }).reduceByKey((accum, n) -> accum + n);

        for(Tuple2<String, Integer> element : stringIntegerJavaPairRDD.collect()){
            System.out.println("("+element._1+", "+element._2+")");
        }
    }
    
    //Подготовить временной ряд с количеством запросов по датам для всех
    //используемых комбинаций http методов и return codes. Исключить из
    //результирующего файла комбинации, где количество событий в сумме было
    //меньше 10.
    private static void taskTwo(JavaRDD<String> rdd) {
        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
        JavaPairRDD<Tuple3<Date, String, String>, Integer> result = rdd.filter(log -> {
            Matcher matcher = compile.matcher(log);
            return matcher.matches();
        }).mapToPair(log -> {
            Matcher matcher = compile.matcher(log);
            if (matcher.find()) {
                return new Tuple2<>(new Tuple3<>(format.parse(matcher.group(4).split(":")[0]), matcher.group(5),
                        matcher.group(8)), 1);
            
            } else {
                return new Tuple2<>(new Tuple3<>(format.parse("01/Jan/1960"), "UNKNOWN", "-1"), 1);
            }
        }).reduceByKey((accum, n) -> accum + n)
          .sortByKey(new Comp(), true).filter(a -> a._2 > 10);
    
        for(Tuple2<Tuple3<Date, String, String>, Integer> element: result.collect()){
            System.out.println("("+element._1._1()+" "+element._1._2()+" "+element._1._3()+", "+element._2+")");
        }
    
    }
    //Произвести расчет скользящим окном в одну неделю количества запросов
    //закончившихся с кодами 4xx и 5xx
    private static void taskThree(JavaRDD<String> rdd) {
        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
        SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        StructField[] structFields = new StructField[]{
                new StructField("date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
        };
        StructType structType = new StructType(structFields);
        SparkSession currentSession = SparkSession.builder().getOrCreate();
        JavaRDD<Row> rowJavaRDD = rdd.filter(log -> {
            Matcher matcher = compile.matcher(log);
            if (matcher.find()) {
                String group = matcher.group(8);
                return group.startsWith("5") || group.startsWith("4");
            } else {
                return false;
            }
        }).map(log -> {
            Matcher matcher = compile.matcher(log);
            if (matcher.find()) {
                return RowFactory.create(newFormat.format(format.parse(matcher.group(4).split(":")[0])).toString(), 1);
            } else {
                return RowFactory.create(newFormat.parse("1960-01-01").toString(), 1);
            }
        });
        Dataset<Row> rowDataset = currentSession.createDataFrame(rowJavaRDD, structType)
                .withColumn("date", col("date").cast("date"))
                .groupBy(window(col("date"), "1 week", "1 day"))
                .agg(sum("count")).orderBy("window");
        Row[] rows = (Row[]) rowDataset.collect();
        for (Row row : rows) {
            System.out.println(row.toString());
        }
    }
    
}
