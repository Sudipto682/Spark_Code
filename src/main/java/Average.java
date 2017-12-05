import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Created by ssah22 on 9/14/2017.
 */
public class Average {

    public static void main(String args[])
    {

        SparkConf sparkConf = new SparkConf().setAppName("CAlculating Average").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.textFile("C:\\Users\\ssah22\\Desktop\\ecom\\internal_product_data.txt");
        JavaPairRDD<Integer,Float> pairs = rdd1.mapToPair(x->{
            String[] attributes = x.split("\\|");
            Float a = Float.parseFloat(attributes[3]);
            Integer id = Integer.parseInt(attributes[0]);
            return new Tuple2<>(id,a);

        });


       /* pairs.foreach(data->{
            System.out.println(data._1+"**********"+data._2);
        });*/


       JavaPairRDD<Integer,Tuple2<Float,Integer>> result = pairs.mapValues(x->{
           return new Tuple2<>(x,1);
       });

      /* result.reduceByKey(new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
           @Override
           public Tuple2<Float, Integer> call(Tuple2<Float, Integer> t1, Tuple2<Float, Integer> t2) throws Exception {
               return new Tuple2<>(t1._1() + 1,t2._2()+1);
           }
       });*/

        JavaPairRDD<Integer,Tuple2<Float,Integer>> r1 =result.reduceByKey( (t1,t2) -> new Tuple2<>(t1._1() + t2._1(),t2._2()+t2._2()));
        JavaPairRDD<Integer,Float> r2 = r1.mapValues(x->x._1/x._2);


       r2.foreach(x->{
           System.out.println(x._1+"******"+x._2);
       });



    }
}
