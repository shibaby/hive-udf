package cn.togeek;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 输入一行两个数组(ids, parentIds)，递归查找id对应的最上层的parentId。
 * 若不存在parentId，则parentId返回-1。
 */
@Description(
   name = "recursive",
   value = "_FUNC_(ids, parentIds) - The parameters are accept two arrays. " +
      "The First is id Array, The Second is id`s parentId Array.\n" +
      "The return value is two columns(id, rootParentId) and many rows. " +
      "When the rootParentId is -1, that means id has no parentId"
)
public class Recursive extends GenericUDTF {

   @Override
   public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
      List<? extends StructField> fieldRefs = argOIs.getAllStructFieldRefs();

      if(fieldRefs.size() != 2) {
         throw new UDFArgumentException("Recursive takes exactly two argument.");
      }

      ObjectInspector[] inspectors = new ObjectInspector[fieldRefs.size()];

      for(int i = 0; i < fieldRefs.size(); i++) {
         inspectors[i] = fieldRefs.get(i).getFieldObjectInspector();
      }

      if(inspectors[0].getCategory() != ObjectInspector.Category.LIST
         || inspectors[1].getCategory() != ObjectInspector.Category.LIST) {
         throw new UDFArgumentException("arguments must be a list / array");
      }

      List<String> fields = new ArrayList<>();
      List<ObjectInspector> fieldsOI = new ArrayList<>();

      fields.add("id");
      fields.add("rootParentId");

      fieldsOI.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldsOI.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

      return ObjectInspectorFactory.getStandardStructObjectInspector(fields, fieldsOI);
   }

   @Override
   public void process(Object[] args) throws HiveException {
      List<Object> ids = (List<Object>) args[0];
      List<Object> parentIds = (List<Object>) args[1];

      List<String> idStrings = new ArrayList<>();
      List<String> parentIdStrings = new ArrayList<>();

      for(Object id: ids) {
         idStrings.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(id));
      }

      for(Object parentId: parentIds) {
         parentIdStrings.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(parentId));
      }

      collectIdsAndRootParentId(idStrings, parentIdStrings);
   }

   private void collectIdsAndRootParentId(List<String> ids, List<String> parentIds) throws HiveException {
      for(int i = 0; i < ids.size(); i++) {
         List<String> columns = new ArrayList<>();
         String id = ids.get(i);
         String parentId = getRootParentId(i, ids, parentIds);

         columns.add(id);
         columns.add(id.equals(parentId) ? "-1" : parentId);

         forward(columns);
      }
   }

   private String getRootParentId(int index, List<String> ids, List<String> parentIds) {
      String parentId = parentIds.get(index);

      if(!"-1".equals(parentId)) {
         index = ids.indexOf(parentId);

         // 若parentId不为-1，但在ids中找不到，那么该条数据为错误数据，parentId标记为-2
         return index < 0 ? "-2" : getRootParentId(index, ids, parentIds);
      }

      parentId = ids.get(index);

      return parentId;
   }

   @Override
   public void close() {

   }
}
