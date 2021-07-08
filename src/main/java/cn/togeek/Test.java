package cn.togeek;

import java.util.ArrayList;
import java.util.List;

public class Test {
   public static void main(String[] args) {
      List<String> list = new ArrayList<>();
      list.add("55");
      list.add("62");
      list.add("8");
      list.add("62");

      System.out.println(list.indexOf("62"));
   }
}
