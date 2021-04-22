package spark.entity.table;

import org.apache.spark.sql.types.StructType;

public abstract class Table {
    private String fileName;
    private String path;

    public Table() {
    }

    public Table(String fileName, String path) {
        this.fileName = fileName;
        this.path = path;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getQualifiedName(){
        return this.path + "/" + this.fileName;
    }

    public  abstract StructType getSchema();
}
