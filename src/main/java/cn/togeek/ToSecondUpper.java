package cn.togeek;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 每次处理单行数据，输出单行数据--处理基本数据类型
 */
public class ToSecondUpper extends UDF {

   public String evaluate(String str) {
      return "hello " + str + " sEcond";
   }

}
