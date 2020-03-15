/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse;


import java.util.Objects;

public class ReadZeroFile {

    private  String filename;
    private  long offset;
    private  long size;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReadZeroFile)) return false;
        ReadZeroFile that = (ReadZeroFile) o;
        return offset == that.offset &&
                size == that.size &&
                Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {

        return Objects.hash(filename, offset, size);
    }

    public ReadZeroFile(String filename, long offset, long size) {
        this.filename = filename;
        this.offset = offset;
        this.size = size;
    }
}
