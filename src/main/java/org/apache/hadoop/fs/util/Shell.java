package org.apache.hadoop.fs.util;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Shell {

  public static String runPrivileged(final String[] args) throws IOException {
    return AccessController.doPrivileged(new PrivilegedAction<String>() {
      @Override
      public String run() {
        try {
          return org.apache.hadoop.util.Shell.execCommand(args);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });
  }
}
