package cn.togeek;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 每次处理单行数据，输出多行/多列数据
 */
public class ExplodeName extends GenericUDTF {

   @Override
   public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
      List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
      ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];
      if(udtfInputOIs.length != 1){
         throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
      }

      for (int i = 0; i < inputFields.size(); i++) {
         udtfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
      }

      if(udtfInputOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
         && ((PrimitiveObjectInspector)udtfInputOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
         throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
      }

      ArrayList<String> fieldNames = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
      fieldNames.add("name");
      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldNames.add("surname");
      fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
   }

   @Override
   public void process(Object[] args) throws HiveException {
      String input = String.valueOf(args[0]);
      String[] name = input.split(" ");
      forward(name); // 1个forward代表一行，可以写多个forward
   }

   @Override
   public void close() throws HiveException {

   }
}
