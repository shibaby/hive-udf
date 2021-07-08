package cn.togeek;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * 每次处理多行/多列数据，输出单行数据
 */
public class ConcatContract extends AbstractGenericUDAFResolver {

   @Override
   public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      if(parameters.length != 1) {
         throw new UDFArgumentLengthException("Exactly one argument is expected");
      }

      if(parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
         throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
            + parameters[0].getTypeName() + " was passed as parameter 1.");
      }
      return new ConcatContractEvaluator();
   }


   @SuppressWarnings({"deprecation", "JavaDoc"})
   static class ConcatContractEvaluator extends GenericUDAFEvaluator {

      private PrimitiveObjectInspector inputOI;
      private StandardListObjectInspector internalMergeOI;
      private StandardListObjectInspector listOI;

      /**
       * 处理输入输出参数类型
       * @param m
       * @param parameters
       * @return
       * @throws HiveException
       */
      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
         super.init(m, parameters);

         if(m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
            inputOI = (PrimitiveObjectInspector) parameters[0];

            return ObjectInspectorFactory.getStandardListObjectInspector(
               ObjectInspectorUtils.getStandardObjectInspector(inputOI)
            );
         }
         else if(m == Mode.PARTIAL2 || m == Mode.FINAL) {
            internalMergeOI = (StandardListObjectInspector) parameters[0];
            inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
            listOI = ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
            return listOI;
         }

         return null;
      }

      static class ArrayAggregationBuffer implements AggregationBuffer {
         List<Object> container;
      }

      /**
       * 创建缓存保存临时结果
       * @return
       */
      @Override
      public AggregationBuffer getNewAggregationBuffer() {
         ArrayAggregationBuffer buffer = new ArrayAggregationBuffer();
         reset(buffer);

         return buffer;
      }

      /**
       * 重置缓存空间
       * @param agg
       */
      @Override
      public void reset(AggregationBuffer agg) {
         ((ArrayAggregationBuffer) agg).container = new ArrayList<>();
      }

      /**
       * 处理输入记录
       * @param agg
       * @param parameters
       */
      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) {
         Object parameter = parameters[0];

         if(parameter != null) {
            putIntoList(parameter, (ArrayAggregationBuffer) agg);
         }
      }

      private void putIntoList(Object parameter, ArrayAggregationBuffer agg) {
         Object paramCopy = ObjectInspectorUtils.copyToStandardObject(parameter, this.inputOI);
         agg.container.add(paramCopy);
      }

      /**
       * 处理局部结果
       * @param agg
       * @return
       */
      @Override
      public Object terminatePartial(AggregationBuffer agg) {
         ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
         ArrayList<Object> list = new ArrayList<>();

         list.addAll(myAgg.container);

         return list;
      }

      /**
       * 聚合局部结果
       * @param agg
       * @param partial
       */
      @Override
      public void merge(AggregationBuffer agg, Object partial) {
         ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
         List<Object> list = (ArrayList<Object>) this.internalMergeOI.getList(partial);

         for(Object obj : list) {
            putIntoList(obj, myAgg);
         }
      }

      /**
       * 输出最终结果
       * @param agg
       * @return
       */
      @Override
      public Object terminate(AggregationBuffer agg) {
         ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
         ArrayList<Object> list = new ArrayList<>();

         list.addAll(myAgg.container);

         return list;
      }
   }
}
