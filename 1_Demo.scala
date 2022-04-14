package request5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.util.Properties
import scala.math.sqrt

object Demo {

  def readDataFromMysql(spark: SparkSession, dataBase: String, username: String, passwd: String): Unit = {

    // TODO 1. 从mysql中读取数据封装为RDD，便于后续处理
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 5
    val urlTmp = "jdbc:mysql://127.0.0.1:3306/"
    val url = urlTmp + dataBase + "?"
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", passwd)

    // 用户收藏表
    val tb_sys_collect = spark.read.jdbc(url, "tb_sys_collect", "id", lowerBound, upperBound, numPartitions, prop).cache()
    tb_sys_collect.createOrReplaceTempView("tb_sys_collect")

    // 用户浏览表
    val tb_user_activity = spark.read.jdbc(url, "tb_user_activity", "id", lowerBound, upperBound, numPartitions, prop).cache()
    tb_user_activity.createOrReplaceTempView("tb_user_activity")


    // 隐式转换
    import spark.implicits._
    // 搞出用户与物品之间的对应关系
    val sql = "SELECT a.user_id,a.activity_id,SUM(a.cou) as score\nFROM (\nSELECT user_id,activity_id,COUNT(*)*5 AS cou\nFROM `tb_sys_collect`\nGROUP BY user_id,activity_id\nUNION\nSELECT user_id,activity_id,COUNT(*) AS cou\nFROM `tb_user_activity`\nGROUP BY user_id,activity_id\n) a \nGROUP BY user_id,activity_id"
    val rdd1 = spark.sql(sql).as[UserActScore].rdd.cache()
    // (user_id,activity_id)  用户id，物品id 评分表
    val user_rdd2 = rdd1.map(f => (f.user_id, f.activity_id)).cache()

    // usersRdd:[id1,id2,...]
    val usersRdd = user_rdd2.map(_._1).distinct().sortBy(x=>x)

    // itemsTemp : [(item_id,index) ... ]
    val itemsTemp = user_rdd2.map(_._2).distinct().sortBy(x=>x).zipWithIndex()

    // 构建物品 id到index的映射map 和  index到id的映射map
    val idxs = itemsTemp.collectAsMap()
    val xsId = itemsTemp.map(f => (f._2, f._1)).collectAsMap()
    // itemsXsRdd : itemsXsRdd:[index1,index2,...]
    val itemsXsRdd = itemsTemp.map(_._2).cache()

    val itemsIdRdd = itemsTemp.map(_._1).distinct().cache()

    // TODO 2. 生成物品相似度矩阵
    //rdd2: （（物品AID，物品BID），1）
    val rdd2 = user_rdd2.join(user_rdd2).map(f => (f._2, 1))
    // 统计相同的物品对出现的次数((（物品AID，物品BID）,同时出现的总数))
    val rdd3 = rdd2.reduceByKey(_ + _)
    //  这里出现了重复  ?????????????????????????
    //  (386,388),1   (388,386),1
    val rdd3_1 = rdd3.sortByKey()

    //对角线上((4,4),4) ((10,10),6)
    val rdd4 = rdd3.filter(f => f._1._1 == f._1._2)
    //    rdd4.map(f=>(f._1._1,f._2))
    //非对角线上的矩阵
    val rdd5 = rdd3.filter(f => f._1._1 != f._1._2)
    //(4,((4,10,2),4))
    val rdd6 = rdd5.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(rdd4.map(f => (f._1._1, f._2)))
    //(10,(4,10,2,5))
    val rdd7 = rdd6.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    //(10,((4,10,3,4),6))
    val rdd8 = rdd7.join(rdd4.map(f => (f._1._1, f._2)))

    //(4,10,3,4,6)  f._3为同时喜欢f._1,f._2的用户数，f._4,f._5为喜欢f._1,f._2的用户数
    val rdd9 = rdd8.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
//    rdd9
//    (16827,15415,1,2,2)
//    (832,15415,1,1,2)
//    (15639,15415,2,3,2)
//    (15839,15415,1,1,2)
//    (12442,15415,2,3,2)
//    (13060,15415,2,3,2)
//    (1268,15415,1,1,2)
//    (1073,15415,2,4,2)
//    (14077,15415,1,2,2)
//    (3901,15415,2,4,2)
//    (1328,15415,2,2,2)
//    (10574,15415,1,1,2)
//    (9983,15415,1,1,2)


    // rdd10 : ((物品A索引，物品B索引)，物品A与B的相似度)
    val rdd10 = rdd9.map(f => ((idxs(f._1), idxs(f._2)), (f._3 / sqrt(f._4 * f._5))))
    val rdd10_22 = rdd9.map(f => ((f._1, f._2), (f._3 / sqrt(f._4 * f._5)))).sortByKey()

    // rdd10_22  验证相似度
//    ((386,387),1.0)
//    ((386,388),1.0)
//    ((386,832),1.0)
//    ((386,1073),0.5)
//    ((386,1328),0.7071067811865475)
//    ((386,3901),0.5)
//    ((386,9983),1.0)
//    ((386,12442),0.5773502691896258)
//    ((386,13060),0.5773502691896258)
//    ((386,14077),0.7071067811865475)
//    ((386,15415),0.7071067811865475)
//    ((386,15639),0.5773502691896258)
//    ((386,16827),0.7071067811865475)
//    ((387,386),1.0)
//    ((387,388),1.0)
//    ((387,832),1.0)
//    ((387,1073),0.5)
//    ((387,1328),0.7071067811865475)
//    ((387,3901),0.5)
//    ((387,9983),1.0)
//    ((387,12442),0.5773502691896258)
//    ((387,13060),0.5773502691896258)
//    ((387,14077),0.7071067811865475)
//    ((387,15415),0.7071067811865475)
//    ((387,15639),0.5773502691896258)
//    ((387,16827),0.7071067811865475)
//    ((388,386),1.0)
//    ((388,387),1.0)
//    ((388,832),1.0)
//    ((388,1073),0.5)

    /**
     * 补充
     */
    // 零集，令其相似度为0，都给补上,相似度集合中，没有的令其相似度为0
    // itemsXsRdd : itemsXsRdd:[index1,index2,...] 物品索引
    val rdd10_1 = itemsXsRdd.cartesian(itemsXsRdd).map(f => ((f._1, f._2), 0.0))
    val rdd10_2 = rdd10_1.union(rdd10).groupByKey().map(f => (f._1, f._2.reduce(_ + _))).cache()

    //排序 将（物品1索引，物品2索引）进行排序
    val rdd11 = rdd10_2.map(f => (f._1._1, (f._1._2, f._2))).sortByKey().map(f => (f._2._1, (f._1, f._2._2))).sortByKey()
    //(物品索引，该物品与其他物品（按索引从小到大排列）相似度的数组)
    val rdd12 = rdd11.map(f => (f._1, f._2._2)).groupByKey().sortByKey().map(f => (f._1, f._2.toArray))
    val rdd13 = rdd12.map(f => IndexedRow(f._1, Vectors.dense(f._2)))
    // rdd14 : 相似度矩阵
    val rdd14 = new IndexedRowMatrix(rdd13)
    // ==================相似度计算正确====================

    // TODO 3. 生成用户对物品的评分矩阵
    // user2 : ((物品ID，用户ID)，评分))
    val user2 = rdd1.map(f => ((f.activity_id, f.user_id), f.score))
    // 物品Id 和 用户Id 作笛卡尔积
    // usersRdd:[id1,id2,...]
    val mm = itemsIdRdd.cartesian(usersRdd)
    //给所有的组合赋值0
    val mm1 = mm.map(f => (f, 0.0))
    //将零集与真实评分集合并
    val mm2 = mm1.union(user2)
    //获取完整的评分集
    // mm3:
//    ((386,4),0.0)
//    ((386,5),0.0)
//    ((386,6),1.0)
//    ((386,7),0.0)
//    ((387,4),0.0)
//    ((387,5),0.0)
//    ((387,6),1.0)
//    ((387,7),0.0)
//    ((388,4),0.0)
//    ((388,5),0.0)
//    ((388,6),1.0)
//    ((388,7),0.0)
//    ((832,4),0.0)
//    ((832,5),0.0)
//    ((832,6),1.0)
//    ((832,7),0.0)
    val mm3 = mm2.groupByKey().map(f => (f._1, f._2.reduce(_ + _))).sortByKey()
    val mm4 = mm3.map(f => (f._1._1, (f._1._2, f._2)))
    // mm5 :（物品索引，（用户ID从小到大）用户对该物品的评分数组）
    val mm5 = mm4.map(f => (f._1, f._2._2)).groupByKey().sortByKey().map(f => (f._1, f._2.toArray)).map(f=>(idxs(f._1),f._2)) // 最后将物品id映射为物品索引
    // mm7 : 评分矩阵
    val mm6 = mm5.map(f => IndexedRow(f._1, (Vectors.dense(f._2))))
    val mm7 = new IndexedRowMatrix(mm6)
    // =================评分矩阵验证正确=================

    // TODO 4. 相似度矩阵 * 评分矩阵 = 推荐列表
    val mm8 = rdd14.toBlockMatrix().multiply(mm7.toBlockMatrix())
    //矩阵转置,并转换为索引矩阵（行标签为用户索引，列标签为物品索引）
    val mm9 = mm8.transpose.toIndexedRowMatrix()
    // 将用户ID与用户索引进行映射
    //(Long,String),用户索引（从0开始的连续整数）与用户ID
    // usersRdd:[id1,id2,...] distinct会打乱顺序
    val users4 = usersRdd.distinct().sortBy(x=>x).zipWithIndex().map(f => (f._2, f._1)).sortByKey()
    /*  val mm10 = mm9.rows.map(f => (f.index, f.vector)).sortByKey()
      .join(users4) // [(index,(vector(score),user_id)) ......]
      .map(f => (f._2._2, f._2._1)) //  [(user_id,vector(score))  ......]
      .map(
        f => {
          val temp = f._2.toArray // 此时的vector[score]是按照物品索引顺序来的，f._2是vector(score)
            .zipWithIndex // 加上Index，方便后续的映射转换
            .map(m => (m._2, m._1)) // [(item_index,score) ...]
            .map(f => (xsId(f._1), f._2)) // 将物品索引映射为物品ID（使用map映射） ->  [(item_id,score) ... ]
            .sortWith(_._2 > _._2).toList // 按照推荐分数进行排序（倒序排列）   -> [(item_id,score) ... ] 按照score从小到大排列
          (f._1, temp)
        }
      )   // 出现bug
    */

    // mm10_1 : [ (user_id,(item_id,score),(item_id,score)) .... ]  注：此时的score是按照倒序排列的
    val mm10_1 = mm9.rows.map(f => (f.index, f.vector)).sortByKey()
      .join(users4) // [(index,(vector(score),user_id)) ......]
      .map(f => (f._2._2, f._2._1)) //  [(user_id,vector(score))  ......]
      .map {
        f => (f._1,f._2.toArray // 此时的vector[score]是按照物品索引顺序来的，f._2是vector(score)
            .zipWithIndex // 加上Index，方便后续的映射转换
            .map(m => (m._2, m._1)) // [(item_index,score) ...]
            .map(f => (xsId(f._1), f._2)) // 将物品索引映射为物品ID（使用map映射） ->  [(item_id,score) ... ]
            .sortWith(_._2 > _._2).toList) // 按照推荐分数进行排序（倒序排列）   -> [(item_id,score) ... ] 按照score从小到大排列
      }


    // TODO 5. 对推荐列表进行去重-（去掉已经评分过的物品）
    // itemFiniMap : [(user_id,(item_id,item_id....)) .....]
    //              每个用户已经评价过的物品，做成一个Map,user_id作为key,iterable(item_id)作为value
    //  user_rdd2 : [(user_id,item_id) ...] 每个用户评价过的物品KVRDD
    val itemFiniMap = user_rdd2.groupByKey().collectAsMap()
    // mm11:[(user_id,array(item_id..))....]  去重后的推荐列表,并去掉score为0的
//    val mm11 = mm101.map {
//      f => {
//        val user_id = f._1
//        // arr : [(item_id,score)]
//        val arr = f._2
//        val arr2 = arr.filter(x => !itemFiniMap.get(user_id).contains(x._1) && x._2 > 0.001).map(x => x._1)
//        (f._1, arr2)
//      }
//    }  // 出现bug，map.get不行

    // mm10_1 : [ (user_id,(item_id,score),(item_id,score)) .... ]  注：此时的score是按照倒序排列的
    val mm11_2 = mm10_1.map {
      f =>
        (f._1,f._2.filter(x => !itemFiniMap(f._1).toArray.contains(x._1)).map(x => x._1))
    }
    // 将array 封装为 string
    // mm12 : [(user_id,"3,5,6").....]
    val mm12 = mm11_2.map(f => {
      (f._1, f._2.mkString(","))
    }).sortByKey()

    // TODO 6. 将结果RDD存入到Mysql中
    val sche = StructType(StructField("user_id", LongType) :: StructField("activity_id", StringType) :: Nil)
    val sqlContext = new SQLContext(spark.sparkContext)
    sqlContext.createDataFrame(mm12.map(f => Row.apply(f._1,f._2)), sche).write.mode(SaveMode.Overwrite).jdbc(url, "tb_user_recommend", prop)


  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    readDataFromMysql(sparkSession, args(0), args(1), args(2))
    // 释放资源
    sparkSession.stop()
  }

}
