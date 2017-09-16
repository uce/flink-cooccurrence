package com.github.uce.flinkcooccurrences;

import java.net.URI;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

class UnsplittableTextInputFormat extends TextInputFormat {

  private static final long serialVersionUID = -790716781723954980L;

  UnsplittableTextInputFormat(URI input) {
    super(new Path(input));
    unsplittable = true;
  }

  @Override
  protected boolean testForUnsplittable(FileStatus pathFile) {
    return true;
  }

}
