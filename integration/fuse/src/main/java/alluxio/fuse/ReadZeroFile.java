package alluxio.fuse;

import alluxio.client.file.FileSystem;

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
