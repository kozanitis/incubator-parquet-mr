package parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class AdamWriter {
  private static final String schemaString =
      "message adamRead {\n"
          + " required int64 id;\n"
          + " required binary dnaString (UTF8);\n"
          + " required int64 mapLocation;\n"
          + " optional int64 randomKey1;\n"
          + " optional int64 randomKey2;\n"
          + "}\n";

  private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

  public static class AdamRead {
    private final long id;
    private final Long randomKey1, randomKey2, mapLocation;
    private final String dnaString;

    public AdamRead(long id, String dnaString, Long mapLocation, Long randomKey1, Long randomKey2){
      this.id = id;
      this.dnaString = dnaString;
      this.mapLocation=mapLocation;
      this.randomKey1 = randomKey1;
      this.randomKey2 = randomKey2;
    }

    public long getId(){    //ID is intended as unique primary key
      return id;
    }

    public String getDNAString(){
      return dnaString;
    }

    public Long getMapLocation(){
      return mapLocation;
    }

    public Long getRandomKey1(){
      return randomKey1;
    }

    public Long getRandomKey2(){
      return randomKey2;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o==null || getClass() != o.getClass()) return false;

      AdamRead other = (AdamRead) o;
      
      if (id == other.id) return true;
      if (dnaString.equals(other.dnaString) && this.mapLocation == other.mapLocation) return true;  //Two DNA strands may have different primary keys but same mapLocation and dna strand
      
      return false;
    }

    @Override
    public int hashCode() {
      long result = (int) (id ^ (id >>> 32));
      result = 31 * result + dnaString.hashCode();
      result = 31 * result + mapLocation.hashCode();
      result = 31 * result + (randomKey1 != null ? randomKey1.hashCode():0);
      result = 31 * result + (randomKey2 != null ? randomKey2.hashCode():0);
      return (int) result;
    }

    
  }

  public static SimpleGroup groupFromRead(AdamRead read) {
    SimpleGroup root = new SimpleGroup(schema);
    root.append("id", read.getId());
    root.append("dnaString", read.getDNAString());
    root.append("mapLocation",read.getMapLocation());
    if (read.getRandomKey1() != null) root.append("randomKey1", read.getRandomKey1());
    if (read.getRandomKey2() != null) root.append("randomKey2", read.getRandomKey2());
    return root;
  }

  public static File writeToFile(List<AdamRead> reads) throws IOException {
  File f = File.createTempFile("AdamReads",".parquet");
  f.deleteOnExit();
  if(!f.delete()) throw new IOException("couldn't delete temp file" + f);

  writeToFile(f, reads);

  return f;
  }

  public static void writeToFile(File f, List<AdamRead> reads) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(f.getAbsolutePath()), conf, new GroupWriteSupport());
    for (AdamRead r: reads) {
      writer.write(groupFromRead(r));
    }
    writer.close();
  }

  public static List<Group> readFile(File f, Filter filter) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    
    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new Path(f.getAbsolutePath()))
                     .withConf(conf)
                     .withFilter(filter)
                     .build();

    Group current;
    List<Group> adamReads = new ArrayList<Group>();

    current = reader.read();
    while (current != null) {
      adamReads.add(current);
      current = reader.read();
    }

    return adamReads;
  }

  public static void main(String[] args) throws IOException {
    File f = new File(args[0]);
    writeToFile(f, TestAdamReadFilters.makeReads());
  }

}
