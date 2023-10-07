package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.apache.spark.sql.types.MetadataBuilder;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
        // read vertices
        java.util.List<Row> vertices_list = new ArrayList<Row>();
        try (BufferedReader br = new BufferedReader(new FileReader("SparkGraphXassignment\\src\\main\\resources\\wiki-vertices.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("	");
                vertices_list.add(RowFactory.create(parts[0], parts[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }	

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		
				StructType vertices_schema = new StructType(new StructField[]{
			new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("article", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		
		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		// read edges
		java.util.List<Row> edges_list = new ArrayList<Row>();		
        try (BufferedReader br = new BufferedReader(new FileReader("SparkGraphXassignment\\src\\main\\resources\\wiki-edges.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("	");
                edges_list.add(RowFactory.create(parts[0], parts[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }	

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
					
		StructType edges_schema = new StructType(new StructField[]{
			new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		
		GraphFrame gf = GraphFrame.apply(vertices,edges);

        // Run pagerank algorithm
        Dataset<Row> pRank = gf.pageRank().resetProbability(0.15).maxIter(10).run().vertices().select("article", "pagerank");
        
        // Order results based on the pagerank score
        pRank = pRank.orderBy(pRank.col("pagerank").desc());

        // Show top 10 results
        pRank.show(10, false);
	}	
}
