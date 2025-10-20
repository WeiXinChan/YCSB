package com.oceanbase.obkv;

import com.yahoo.ycsb.Client;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RunMain {
    public static void main(String[] args) {
        try {
            List<String> list = new ArrayList<>();
            list.add("-s");
            list.addAll(Arrays.asList(args));
            String[] arr = list.toArray(new String[0]);
            Method method = Client.class.getMethod("main", String[].class);
            method.invoke(null, new Object[]{arr});
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
