package parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import parquet.example.data.Group;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.LongColumn;
import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.filter2.recordlevel.AdamWriter.AdamRead;
import parquet.io.api.Binary;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.longColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;

// AdamRead header: AdamRead(unique long id, required String dnaString, required long mapLocation, optional Long randomKey1, optional Long randomKey2)

public class TestAdamReadFilters {

  private static long mapLowest, mapHighest, mapRange;

  // Used for mapBounds specific to lowest and highest possible map locations
  private static void setMapBounds(long low, long high){
    mapLowest = low;
    mapHighest = high;
    mapRange = mapHighest-mapLowest;
  }

  private static String randomDNAGenerator(){
    return randomDNAGenerator(20);
  }

  private static String randomDNAGenerator(long maxLength) {
    String dnaSequence = new String();
    long length = (long)(Math.random()*(maxLength-5))+5;
    for(long i=0; i<length; i++)
    {
        switch((int)(Math.random()*4))
        {
          case 0:
            dnaSequence+="A";
            break;
          case 1:
            dnaSequence+="G";
            break;
          case 2:
            dnaSequence+="C";
            break;
          case 3:
            dnaSequence+="T";
            break;
        }
    }
    return dnaSequence;
  }

  private static Long[] randomMapLocations(int numLocations){
    return randomMapLocations(numLocations, -100, 100);
  }

  private static Long[] randomMapLocations(int numLocations, long mapLocationLowest, long mapLocationHighest){
    Long[] mapLocations = new Long[numLocations];
    setMapBounds(mapLocationLowest, mapLocationHighest);
    for(int i=0; i<numLocations; i++)
    {
      mapLocations[i]=(long)(Math.random()*(mapRange+1))+mapLocationLowest;
    }
    Arrays.sort(mapLocations);
    return mapLocations;
  }

  private static Long randomKeyGen(long maxKey){
    return new Long((long)(Math.random()*(maxKey-1))+1);
  }

  public static List<AdamRead> makeReads() {
    List<AdamRead> reads = new ArrayList<AdamRead>();
    long maxRandKey1 = 300;
    long maxRandKey2 = 200;
    int numReads = 10;
    Long[] mapLocations = randomMapLocations(numReads);
    String[] dnaSequences = new String[numReads];
    long[] ids = new long[numReads];
    int j=1,k=numReads*2;
    for(int i=0; i<numReads; i++)
    {
      dnaSequences[i]=randomDNAGenerator();
      if(i%2==0) {
        ids[i]=j;
        j+=2;
      }
      else {
        ids[i]=k;
        k-=2;
      }
    }
    // Manually set a few DNA Sequences
    dnaSequences[2]="AGTCACTG";
    dnaSequences[6]="CTGACTA";
    dnaSequences[8]="TCAGCTACG";
    dnaSequences[9]="TCAGCTACG";

    return makeReads(ids, mapLocations, dnaSequences, maxRandKey1, maxRandKey2);
  }

  public static List<AdamRead> makeReads(long[] id, Long[] mapLocations, String[] dnaSequences, long maxKey1, long maxKey2){
    List<AdamRead> reads = new ArrayList<AdamRead>();
    int numReads = mapLocations.length;
    
    for(int i=0; i<numReads; i++)
    {
      reads.add(new AdamRead(id[i], dnaSequences[i], mapLocations[i], randomKeyGen(maxKey1), randomKeyGen(maxKey2)));
    }
    return reads;
  }

  private static File adamFile;
  private static List<AdamRead> adamReads;

  @BeforeClass
  public static void setup() throws IOException{
    adamReads = makeReads();
    adamFile = AdamWriter.writeToFile(adamReads); 
  }

  private static interface ReadFilter {
    boolean keep(AdamRead r);
  }

  private static List<Group> getExpected(ReadFilter f){
    List<Group> expected = new ArrayList<Group>();
    for (AdamRead r: adamReads){
      if(f.keep(r)) {
        expected.add(AdamWriter.groupFromRead(r));
      }
    }
    return expected;
  }

  private static void assertFilter(List<Group> found, ReadFilter f){
    List<Group> expected = getExpected(f);
    assertEquals(expected.size(), found.size());
    Iterator<Group> expectedIter = expected.iterator();
    Iterator<Group> foundIter = found.iterator();
    while(expectedIter.hasNext()){
      assertEquals(expectedIter.next().toString(), foundIter.next().toString());
    }
  }

  @Test
  public void testNoFilter() throws Exception {
    List<Group> found = AdamWriter.readFile(adamFile, FilterCompat.NOOP);
    assertFilter(found, new ReadFilter(){
      @Override
      public boolean keep(AdamRead r) {
        return true;
      }
    });
  }

  @Test
  public void testAllFilter() throws Exception {
    BinaryColumn dna = binaryColumn("dnaString");
    
    FilterPredicate pred = eq(dna, Binary.fromString("XYXYXY"));

    List<Group> found = AdamWriter.readFile(adamFile, FilterCompat.get(pred));

    assertEquals(new ArrayList<Group>(), found);
  }

  @Test
  public void testRandomKey1NotNull() throws Exception {
    LongColumn key = longColumn("randomKey1");

    FilterPredicate pred = notEq(key, null);

    List<Group> found = AdamWriter.readFile(adamFile, FilterCompat.get(pred));

    assertFilter(found, new ReadFilter() {
      @Override
      public boolean keep(AdamRead r) {
        return r.getRandomKey1() != null;
      }
    });
  }

  public static class rangeMaps extends UserDefinedPredicate<Long> {
  
    private static long lower = 1;
    private static long higher = -1;

    public static void setRange(long low, long high){
      lower = low;
      higher = high;
    }
    
    @Override
    public boolean keep(Long value){
      return (value<higher && value>lower);
    }

    @Override
    public boolean canDrop(Statistics<Long> statistics){
      return (statistics.getMax()<lower || statistics.getMin()>higher);
    }

    @Override
    public boolean inverseCanDrop(Statistics<Long> statistics){
      return !canDrop(statistics);
    }
  }

  @Test
  public void testRangeMaps() throws Exception {
    // Uses the static variables to get the middle of the range
    testRangeMaps((long)(mapRange/4)+mapLowest, (long)(mapRange*3/4)+mapLowest);
  }

  public void testRangeMaps(long bottom, long top) throws Exception {
    LongColumn mapLocation = longColumn("mapLocation");
    
    final long lower=bottom, upper=top;

    rangeMaps.setRange(bottom, top);

    FilterPredicate pred = userDefined(mapLocation, rangeMaps.class);

    List<Group> found = AdamWriter.readFile(adamFile, FilterCompat.get(pred));

    assertFilter(found, new ReadFilter() {
      @Override
      public boolean keep(AdamRead r) {
        return (r.getMapLocation() > lower && r.getMapLocation() < upper);
      }
    });
  }
}
