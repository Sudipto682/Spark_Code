import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ssah22 on 9/12/2017.
 */
public class Test1 {

        public static void main(String args[])
        {
            SparkConf sparkConf = new SparkConf().setAppName("Reading data csv").setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            JavaRDD<String> data = sc.textFile("C:\\Users\\ssah22\\Desktop\\ecom\\ecom_competitor_data.txt");
            JavaRDD<String> data2 = sc.textFile("C:\\Users\\ssah22\\Desktop\\ecom\\internal_product_data.txt");
            JavaRDD<String> data3 = sc.textFile("C:\\Users\\ssah22\\Desktop\\ecom\\seller_data.txt");
            SQLContext sqlContext = new SQLContext(sc);

       /* JavaRDD<competitor> record  = sc.textFile("C:\\Users\\ssah22\\Desktop\\ecom\\ecom_competitor_data.txt").map(
                new Function<String, competitor>() {
                    public competitor call(String line) throws Exception {

                        String[] fields = line.split("\\|");
                        //competitor c = new competitor(Integer.parseInt(fields[0].trim()),Integer.parseInt(fields[1].trim()),fields[2],fields[3],Long.parseLong(fields[4].trim()));
                        competitor c = new competitor();
                        c.setProd_id(fields[0]);
                        c.setPrice(fields[1]);
                        c.setSaleEvent(fields[2]);
                        c.setRival_name(fields[3]);
                        c.setTs(fields[4]);

                        return c;

                    }

                });
        List<competitor> list =record.collect();
        System.out.println(Arrays.toString(list.toArray()));*/

            sqlContext.udf().register("function", new UDF6<String, String, String, String,String,String, Double>() {

                @Override
                public Double call(String P, String X, String Y, String Q,String category,String net_value) throws Exception {
                    double p = Double.parseDouble(P);
                    double x = Double.parseDouble(X);
                    double y = Double.parseDouble(Y);
                    double q = Double.parseDouble(Q);

                    if((p+y)<q)
                        return p+y;
                    else if ((p+x)<q && q<(p+y))
                        return q;
                    else if(p<q && q<(p+x) && category.equals("Special"))
                        return q;
                    else if(q<p)
                    {
                        if(net_value.equals("High"))
                            return 0.9*p;
                        else
                            return p;
                    }
                    return null;
                }
            }, DataTypes.DoubleType);

            SparkSession spark = SparkSession
                    .builder()
                    .appName("Java Spark SQL ")
                    .getOrCreate();


            //generating the schema
            /*String schemaString = "prod_id price Sale_Event Rival_name ts";
            List<StructField> fields = new ArrayList<>();
            for(String fieldname:schemaString.split(" "))
            {
                StructField field = DataTypes.createStructField(fieldname,DataTypes.StringType,true);
                fields.add(field);
            }

            StructType schema = DataTypes.createStructType(fields);
*/
            StructType schema = DataTypes
                    .createStructType(new StructField[] {
                            DataTypes.createStructField("prod_id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("price", DataTypes.DoubleType, false),
                            DataTypes.createStructField("Sale_Event", DataTypes.StringType, true),
                            DataTypes.createStructField("Rival_name", DataTypes.StringType, true),
                            DataTypes.createStructField("ts", DataTypes.StringType, true) });



        /*//generating rows from java rdd
        JavaRDD<Row> rowRDD = data.map(new Function<String, Row>()
                                       {
                                           public Row call(String record)
                                           {
                                               String[] attributes = record.split("\\|");
                                               return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4]);
                                           }
                                       }
        );*/

            JavaRDD<Row> rowRDD = data.map(x -> {
                String[] attributes = x.split("\\|");
                return RowFactory.create(Integer.parseInt(attributes[0]),Double.parseDouble(attributes[1]) , attributes[2], attributes[3], attributes[4]);
            });



            Dataset<Row> competitor_df = spark.createDataFrame(rowRDD,schema);
            competitor_df.createOrReplaceTempView("competitor");
            Dataset<Row> competitor_sql = spark.sql("select * from competitor");
            competitor_sql.show();

           /* //generating schema for table2
            String schemaString2 = "prod_id category s_category pv min_margin max_margin seller_id l_modified";
            List<StructField> fields2 = new ArrayList<>();
            for(String fieldname:schemaString2.split(" "))
            {
                StructField field = DataTypes.createStructField(fieldname,DataTypes.StringType,true);
                fields2.add(field);
            }

            StructType schema2 = DataTypes.createStructType(fields2);


            JavaRDD<Row> rowRDD2 = data2.map( x -> {
                String[] attributes = x.split("\\|");
                return RowFactory.create(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4],attributes[5],attributes[6],attributes[7]);
            });

            Dataset<Row> internal_df = spark.createDataFrame(rowRDD2,schema2);
            internal_df.createOrReplaceTempView("internal");
            Dataset<Row> internal_sql = spark.sql("select * from internal");
            internal_sql.show();


            //generating schema for table3
            String schemaString3 = "seller_id type net_value since user_rating last_sold category";
            List<StructField> fields3 = new ArrayList<>();
            for(String fieldname:schemaString3.split(" "))
            {
                StructField field = DataTypes.createStructField(fieldname,DataTypes.StringType,true);
                fields3.add(field);
            }

            StructType schema3 = DataTypes.createStructType(fields3);


            JavaRDD<Row> rowRDD3 = data3.map( x -> {
                String[] attributes = x.split("\\|");
                return RowFactory.create(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4],attributes[5],attributes[6]);
            });

            Dataset<Row> seller_df = spark.createDataFrame(rowRDD3,schema3);


            seller_df.createOrReplaceTempView("seller");
            Dataset<Row> seller_sql = spark.sql("select * from seller");
            seller_sql.show();*/
            /*competitor_df.groupBy("prod_id").min("price").agg();
            competitor_df.show();
*/

            //sql logic to retrieve the result
        /*Dataset<Row> ecom_1 = spark.sql("with table1 as (select prod_id,min(price) as Q from competitor group by prod_id) select b.Q,b.prod_id,a.Sale_Event,a.Rival_name,a.ts from competitor a join table1 as b on a.prod_id=b.prod_id and a.price=b.Q");
        ecom_1.createOrReplaceTempView("min_price");
        ecom_1.show();
        Dataset<Row> ecom_internal = spark.sql("select a.prod_id,Q,Sale_Event,Rival_name,ts,pv,min_margin,max_margin,seller_id from min_price a, internal b where a.prod_id = b.prod_id");
        ecom_internal.createOrReplaceTempView("Ecom_Internal");
        ecom_internal.show();
        Dataset<Row> e_i_s = spark.sql("select prod_id,Q,Sale_Event,Rival_name,ts,pv,min_margin,max_margin,a.seller_id,net_value from Ecom_Internal a,seller b where a.seller_id=b.seller_id");
        e_i_s.createOrReplaceTempView("merged_table");
        e_i_s.show();
        e_i_s.schema();
        Dataset<Row> result = spark.sql("select prod_id,function(pv,min_margin,max_margin,Q,Sale_Event,net_value) as MS_Price, ts, Q as Cheapest_Price,Rival_name from merged_table");
        result.show();*/


        }


    }


