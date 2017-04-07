package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;

public class RawLocalProxyFs extends RawLocalFs {
  
  public RawLocalProxyFs(final Configuration conf) throws IOException,
      URISyntaxException {
    super(conf);
  }
}
