package site.ycsb.db.obkv;

import site.ycsb.Client;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RunMain {
  public static void main(String[] args) throws Exception {
    Method parseMethod = Client.class.getDeclaredMethod("parseArguments", String[].class);
    parseMethod.setAccessible(true);
    Properties properties = (Properties) parseMethod.invoke(null, new Object[]{args});
    List<String> argsList = new ArrayList<>(Arrays.asList(args));
    if (!properties.containsKey(Client.EXPORT_FILE_PROPERTY)) {
      DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
      String dateStr = LocalDateTime.now().format(format);
      String fileName = String.format("%s_%s_result", "obkv", dateStr);
      Path path = Paths.get(fileName);
      Files.deleteIfExists(path);
      Files.createFile(path);
      argsList.add("-p");
      argsList.add(Client.EXPORT_FILE_PROPERTY + "=" + fileName);
      System.out.println("the result will be written in " + path);
    }
    Method method = Client.class.getMethod("main", String[].class);
    method.invoke(null, new Object[]{argsList.toArray(new String[0])});
  }
}
