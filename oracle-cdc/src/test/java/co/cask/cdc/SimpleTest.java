package co.cask.cdc;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SimpleTest {
  public static void main(String[] args) {
    List<String> list = new ArrayList<>();
    list.add("vinisha");
    list.add("priyanka");
    list.add("kumud");
    list.add("vinay");

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < list.size(); i++) {
      sb.append(list.get(i));
      if (i < list.size() - 1) {
        sb.append(",");
      }
    }
    System.out.println("string is: " + sb.toString());

  }
}
