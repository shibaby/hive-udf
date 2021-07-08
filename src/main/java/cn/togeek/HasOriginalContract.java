package cn.togeek;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * 每次处理单行数据，输出单行数据--处理复杂数据类型
 */
public class HasOriginalContract extends GenericUDF {

   ListObjectInspector listOI;
   WritableStringObjectInspector elementsOI;
   WritableStringObjectInspector argOI;

   @Override
   public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
      if(arguments.length != 2) {
         throw new UDFArgumentLengthException("HasOriginalContract only takes 2 arguments: List<T>, T");
      }

      ObjectInspector a = arguments[0];
      ObjectInspector b = arguments[1];

      System.out.println(a);
      System.out.println(b);

      if(!(a instanceof ListObjectInspector) || !(b instanceof WritableStringObjectInspector)) {
         throw new UDFArgumentException("First argument must be a list / array, Second argument must be a string");
      }

      this.listOI = (ListObjectInspector) a;
      this.elementsOI = (WritableStringObjectInspector) this.listOI.getListElementObjectInspector();
      this.argOI = (WritableStringObjectInspector) b;

      return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
   }

   @Override
   public Object evaluate(DeferredObject[] arguments) throws HiveException {
      int listLength = this.listOI.getListLength(arguments[0].get());
      Text larg = (Text) arguments[1].get();
      String arg = String.valueOf(argOI.getPrimitiveJavaObject(larg));

      // 一行数据中第1列与第2列的关系
      for(int i = 0; i < listLength; i++) {
         Text lelement = (Text) this.listOI.getListElement(arguments[0].get(), i);
         String element = String.valueOf(elementsOI.getPrimitiveJavaObject(lelement));

         if(arg.equals(element)) {
            return true;
         }
      }

      return false;
   }

   @Override
   public String getDisplayString(String[] children) {
      return "HasOriginalContract throws an Exception";
   }
}
