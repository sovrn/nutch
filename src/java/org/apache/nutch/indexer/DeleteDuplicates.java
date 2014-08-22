/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer;

import java.io.*;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolBase;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;

/**
 * Delete duplicate documents in a set of Lucene indexes.
 * Duplicates have either the same contents (via MD5 hash) or the same URL.
 * 
 * This tool uses the following algorithm:
 * 
 * <ul>
 * <li><b>Phase 1 - remove URL duplicates:</b><br/>
 * In this phase documents with the same URL
 * are compared, and only the most recent document is retained -
 * all other URL duplicates are scheduled for deletion.</li>
 * <li><b>Phase 2 - remove content duplicates:</b><br/>
 * In this phase documents with the same content hash are compared. If
 * property "dedup.keep.highest.score" is set to true (default) then only
 * the document with the highest score is retained. If this property is set
 * to false, only the document with the shortest URL is retained - all other
 * content duplicates are scheduled for deletion.</li>
 * <li><b>Phase 3 - delete documents:</b><br/>
 * In this phase documents scheduled for deletion are marked as deleted in
 * Lucene index(es).</li>
 * </ul>
 * 
 * @author Andrzej Bialecki
 */
public class DeleteDuplicates extends ToolBase
  implements Mapper, Reducer, OutputFormat {
  private static final Log LOG = LogFactory.getLog(DeleteDuplicates.class);

//   Algorithm:
//      
//   1. map indexes -> <url, <md5, url, time, urlLen, index,doc>>
//      reduce, deleting all but most recent
//
//   2. map indexes -> <md5, <md5, url, time, urlLen, index,doc>>
//      partition by md5
//      reduce, deleting all but with highest score (or shortest url).

  public static class IndexDoc implements WritableComparable {
    private Text url = new Text();
    private int urlLen;
    private float score;
    private long time;
    private String[] reports;
    private MD5Hash hash = new MD5Hash();
    private Text index = new Text();              // the segment index
    private int doc;                              // within the index
    private boolean keep = true;                  // keep or discard

    public String toString() {
      return "[url=" + url + ",score=" + score + ",time=" + time
        + ",hash=" + hash + ",index=" + index + ",doc=" + doc
        + ",keep=" + keep + "]";
    }
    
    public void write(DataOutput out) throws IOException {
      url.write(out);
      out.writeFloat(score);
      out.writeLong(time);
      hash.write(out);
      index.write(out);
      out.writeInt(doc);
      out.writeBoolean(keep);
      if ( reports == null )
         reports = new String[0];
      out.writeInt(reports.length);
      for ( String report : reports )
         ObjectWritable.writeObject(out,report,String.class,null);
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      urlLen = url.getLength();
      score = in.readFloat();
      time = in.readLong();
      hash.readFields(in);
      index.readFields(in);
      doc = in.readInt();
      keep = in.readBoolean();
      int length = in.readInt();
      reports = new String[length];
      for ( int i=0; i<length; i++ )
         reports[i] = (String)ObjectWritable.readObject(in,null);
    }

    public int compareTo(Object o) {
      IndexDoc that = (IndexDoc)o;
      if (this.keep != that.keep) {
        return this.keep ? 1 : -1; 
      } else if (!this.hash.equals(that.hash)) {       // order first by hash
        return this.hash.compareTo(that.hash);
      } else if (this.time != that.time) {      // prefer more recent docs
        return this.time > that.time ? 1 : -1 ;
      } else if (this.urlLen != this.urlLen) {  // prefer shorter urls
        return this.urlLen - that.urlLen;
      } else {
        return this.score > that.score ? 1 : -1;
      }
    }

    public boolean equals(Object o) {
      IndexDoc that = (IndexDoc)o;
      return this.keep == that.keep
        && this.hash.equals(that.hash)
        && this.time == that.time
        && this.score == that.score
        && this.urlLen == that.urlLen
        && this.index.equals(that.index) 
        && this.doc == that.doc;
    }

    public boolean hasReports() {
      return( reports.length > 0 );
    }
  }

  public static class InputFormat extends InputFormatBase {
    private static final long INDEX_LENGTH = Integer.MAX_VALUE;

    /** Return each index as a split. */
    public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
      //***************************************************************************************
      // LIJIT: Don't list paths in the input directories; the input paths
      //         are the paths to the indexes themselves
      Path[] files = job.getInputPaths();
      if ( FileSystem.get(job).exists( new Path( files[0], "part-0000" ) ) )
      {
         // use the old behavior (listPaths(job)) if doing unit tests
         LOG.info( "part-0000 exists in " + files[0] + "; looks like we're performing unit tests" );
         files = listPaths(job);
      }
      // /LIJIT
      InputSplit[] splits = new InputSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        splits[i] = new FileSplit(files[i], 0, INDEX_LENGTH, job);
      }
      return splits;
    }

    public class DDRecordReader implements RecordReader {

      private IndexReader indexReader;
      private int maxDoc = 0;
      private int doc = 0;
      private Text index;

      public DDRecordReader(FileSplit split, JobConf job,
          Text index) throws IOException {
       try {
          indexReader = IndexReader.open(new FsDirectory(FileSystem.get(job), split.getPath(), false, job));
          maxDoc = indexReader.maxDoc();
       } catch (IOException ioe) {
          LOG.warn("Can't open index at " + split + ", skipping. (" + ioe.getMessage() + ")");
          indexReader = null;
       }

        this.index = index;
      }

      public boolean next(Writable key, Writable value)
        throws IOException {

        // skip empty indexes
        if (indexReader == null || maxDoc <= 0)
           return false;

        // skip deleted documents
        //***************************************************************************************
        // LIJIT: fix this bug:
        //while (indexReader.isDeleted(doc) && doc < maxDoc) doc++;
        while (doc < maxDoc && indexReader.isDeleted(doc)) doc++;
        // /LIJIT
        if (doc >= maxDoc)
          return false;

        Document document = indexReader.document(doc);
        // fill in key
        ((Text)key).set(document.get("url"));
        // fill in value
        IndexDoc indexDoc = (IndexDoc)value;
        indexDoc.keep = true;
        indexDoc.url.set(document.get("url"));
        indexDoc.hash.setDigest(document.get("digest"));
        indexDoc.score = Float.parseFloat(document.get("boost"));
        indexDoc.reports = document.getValues("lijitinformer");
        try {
          indexDoc.time = DateTools.stringToTime(document.get("tstamp"));
        } catch (Exception e) {
          // try to figure out the time from segment name
          try {
            String segname = document.get("segment");
            indexDoc.time = new SimpleDateFormat("yyyyMMddHHmmss").parse(segname).getTime();
            // make it unique
            indexDoc.time += doc;
          } catch (Exception e1) {
            // use current time
            indexDoc.time = System.currentTimeMillis();
          }
        }
        indexDoc.index = index;
        indexDoc.doc = doc;

        doc++;

        return true;
      }

      public long getPos() throws IOException {
        return maxDoc == 0 ? 0 : (doc*INDEX_LENGTH)/maxDoc;
      }

      public void close() throws IOException {
        indexReader.close();
      }
      
      public WritableComparable createKey() {
        return new Text();
      }
      
      public Writable createValue() {
        return new IndexDoc();
      }

      public float getProgress() throws IOException {
        return maxDoc == 0 ? 0.0f : (float)doc / (float)maxDoc;
      }
    }
    
    /** Return each index as a split. */
    public RecordReader getRecordReader(InputSplit split,
                                        JobConf job,
                                        Reporter reporter) throws IOException {
      FileSplit fsplit = (FileSplit)split;
      Text index = new Text(fsplit.getPath().toString());
      reporter.setStatus(index.toString());
      return new DDRecordReader(fsplit, job, index);
    }
  }
  
  public static class HashPartitioner implements Partitioner {
    public void configure(JobConf job) {}
    public void close() {}
    public int getPartition(WritableComparable key, Writable value,
                            int numReduceTasks) {
      int hashCode = ((MD5Hash)key).hashCode();
      return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
  
  /****************************************************************
  * LIJIT:
  * The URLs reducer deletes duplicates based on the page URL.  The dedup process implemented
  * here diverges substantially from the original dedup process.  (Eventually we need to write
  * our own instead of continually hacking this one.)
  *
  * The reduce method is invoked for every individual URL in the index, and the "values" iterator
  * contains the document information for all documents with that URL.  So in the following
  * description of iterating over documents, these are all documents in the index with the
  * same URL.
  *
  * Each document contains the list of the reports that have ever been made on the page.  The
  * individual reports contain the informer hash, report type, and report URL keys of the
  * original report.  Thus, if the report keys do not match the URL of the page, we can know
  * that the original URL request (whatever it was) was redirected the crawler/indexer to
  * the actual page we're examining now.
  *
  * Because of this, it is possible for those documents in the index to contain an incomplete
  * set of reports on that page.  If we delete some of these documents, search results would
  * suffer.  So we want to keep the newest index document that has unique report informers.
  *
  * We begin by sorting the values by timestamp in descending order.  Thus, the newest documents
  * are at the front of the list.
  *
  * Then we make a first pass through the documents.  If we find one that has keys that match
  * the URL, we know that's the best document to keep.  At the time it was crawled, we will
  * have gotten a complete set of reports that were directly on that page.  Other reports
  * on pages that redirect to the page in question likely were not captured.
  *
  * So now we have the first document we're going to keep.  For each document we keep, we
  * also make note of all the informers that have reported on it.  Then, as we go through the
  * rest of the documents, we only keep a document if that document has an informer on it
  * that is not on one of the other documents with that URL that we've already kept.
  *
  * LIJIT
  ****************************************************************/
  public static class UrlsReducer implements Reducer {
    
    public void configure(JobConf job) {}
    
    public void close() {}
    
    public void reduce(WritableComparable key, Iterator values,
        OutputCollector output, Reporter reporter) throws IOException {

      String url = key.toString();

      // Sort all of the documents with this URL.
      ArrayList<IndexDoc> docs = new ArrayList<IndexDoc>();
      while ( values.hasNext() )
         docs.add( (IndexDoc)values.next() );
      Collections.sort( docs, new IndexDocTimeComparator() );

      if ( LOG.isDebugEnabled() ) LOG.debug( "------------------------------------------------------------\n                                                                      document: " + url );

      // Go through the documents and find the newest one that has a report whose
      // keys match the URL.  This is the best document to keep from things like
      // blog feeds.
      IndexDoc firstDoc = null;
      Iterator<IndexDoc> docsIter = docs.iterator();
      while ( docsIter.hasNext() && firstDoc == null )
      {
         IndexDoc doc = docsIter.next();
         if ( hasMatchingReport( doc ) )
            firstDoc = doc;
      }

      // Iterate through the documents in the index with this URL.  Always keep
      // the newest one, then check out the informers on the rest of them to
      // decide if we need to keep any of the older ones.
      HashMap<String,String> informers = new HashMap<String,String>();
      docsIter = docs.iterator();
      while ( docsIter.hasNext() )
      {
         IndexDoc doc = docsIter.next();
         if ( !doc.hasReports() )             // Throw away documents that have no reports at all
         {
            LOG.info( "discarding document with no informers: " + url );
            discardDoc( doc, output );
         }
         else if ( doc == firstDoc )          // always keep the newest one
         {
            LOG.debug( "Keeping first doc" );
            keepDoc( doc, output, informers );
         }
         else              // Finally, only keep a document if it has new informers
         {
            keepIfNewInformers( doc, output, informers );
         }
      }
    }
  }
  
  /****************************************************************
  * See if there is a report on this document with keys that match the URL
  *
  ****************************************************************/
  private static boolean hasMatchingReport( IndexDoc doc )
  {
     boolean hasMatch = false;
     for ( String reportStr : doc.reports )
     {
        ReportInfo info = new ReportInfo( doc.url.toString(), reportStr );
        if ( info.keysMatchURL() )
        {
           LOG.debug( "Found document with report keys that match URL" );
           hasMatch = true;
        }
     }
     return( hasMatch );
  }

  /****************************************************************
  * Keep a document if there are previously unknown informers that have made a report on the page.
  * Strictly speaking, only site reports and blog post reports are important to us.  This may
  * be overly restrictive, but we'll go with that for now.
  *
  ****************************************************************/
  private static void keepIfNewInformers( IndexDoc doc, OutputCollector output, HashMap<String,String> informers ) throws IOException
  {
     boolean keep = false;
     String url = doc.url.toString();
     if ( doc != null )
     {
        // Go through all the informers on a document and see if we've already kept a document
        // with its URL with the same informer.  If not, we'll have to keep this one too.
        for ( String reportStr : doc.reports )
        {
           ReportInfo info = new ReportInfo( url, reportStr );
           if ( !informers.containsKey( info.informerHash ) )
           {
              if ( info.isSiteReport() ||
                   ( !info.keysMatchURL() && info.isBlogPostReport() ) )
              {
                 if ( LOG.isDebugEnabled() ) LOG.debug( "keep because of report: " + info );
                 keep = true;
              }
           }
        }
     }

     if ( keep )
        keepDoc( doc, output, informers );
     else
        discardDoc( doc, output );
  }

  private static void discardDoc( IndexDoc doc, OutputCollector output ) throws IOException
  {
    if ( doc != null )
    {
      doc.keep = false;
      if ( LOG.isDebugEnabled() ) LOG.debug( "-discard " + doc );
      output.collect( doc.hash, doc );

      if ( LOG.isDebugEnabled() )
      {
         for ( String reportStr : doc.reports )
            LOG.debug( "discard doc report: " + reportStr );
      }
    }
  }

  private static void keepDoc( IndexDoc doc, OutputCollector output, HashMap<String,String> informers ) throws IOException
  {
    if ( doc != null )
    {
      doc.keep = true;
      if ( LOG.isDebugEnabled() ) LOG.debug( "-keep    " + doc );
      output.collect( doc.hash, doc );

      // Remember all the informers on a document we're going to keep for 
      // later comparison with other documents with the same URL.
      for ( String reportStr : doc.reports )
      {
         if ( LOG.isDebugEnabled() ) LOG.debug( "keep doc report: " + reportStr );
         ReportInfo info = new ReportInfo( doc.url.toString(), reportStr );
         informers.put( info.informerHash, info.informerHash );
      }
    }
  }

  private static class ReportInfo
  {
    String url;
    String informerHash;
    int reportRelation;
    String reportURLKeys;

    public ReportInfo( String url, String informerField )
    {
      this.url = url;
      String[] pieces = informerField.split( "," );
      informerHash = pieces[0];
      reportRelation = Integer.parseInt( pieces[1] );
      reportURLKeys = pieces[2];
    }

    public boolean isBlogPostReport()
    {
      return( reportRelation == 2 );
    }

    public boolean isSiteReport()
    {
      return( reportRelation == 8 );
    }

    public boolean keysMatchURL()
    {
      return( reportURLKeys.equals( makeKeysString( url ) ) );
    }

    public String toString()
    {
      return( informerHash + ',' + reportRelation + ',' + reportURLKeys );
    }
  }
  
  public static class HashReducer implements Reducer {
    boolean byScore;
    
    public void configure(JobConf job) {
      byScore = job.getBoolean("dedup.keep.highest.score", true);
    }
    
    public void close() {}
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter)
      throws IOException {
      IndexDoc highest = null;
      while (values.hasNext()) {
        IndexDoc value = (IndexDoc)values.next();
        // skip already deleted
        if (!value.keep) {
          LOG.debug("-discard " + value + " (already marked)");
          output.collect(value.url, value);
          continue;
        }
        if (highest == null) {
          highest = value;
          continue;
        }
        IndexDoc toDelete = null, toKeep = null;
        boolean metric = byScore ? (value.score > highest.score) : 
                                   (value.urlLen < highest.urlLen);
        if (metric) {
          toDelete = highest;
          toKeep = value;
        } else {
          toDelete = value;
          toKeep = highest;
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("-discard " + toDelete + ", keep " + toKeep);
        }
        
        toDelete.keep = false;
        output.collect(toDelete.url, toDelete);
        highest = toKeep;
      }    
      LOG.debug("-keep " + highest);
      // no need to add this - in phase 2 we only process docs to delete them
      // highest.keep = true;
      // output.collect(key, highest);
    }
  }
    
  private FileSystem fs;

  public void configure(JobConf job) {
    setConf(job);
  }
  
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {}

  /** Map [*,IndexDoc] pairs to [index,doc] pairs. */
  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    IndexDoc indexDoc = (IndexDoc)value;
    // don't delete these
    if (indexDoc.keep) return;
    // delete all others
    output.collect(indexDoc.index, new IntWritable(indexDoc.doc));
  }

  /** Delete docs named in values from index named in key. */
  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    Path index = new Path(key.toString());
    // LIJIT: need to do this because IndexReader calls "seek()" on FsDirectory, and
    // a Nutch FsDirectory does not support seek().  Lucene FSDirectory does.
    // This does mean we can never use a Hadoop DFS any more.
    IndexReader reader = IndexReader.open(FSDirectory.getDirectory(index.toString()));
    //IndexReader reader = IndexReader.open(new FsDirectory(fs, index, false, getConf()));
    // end LIJIT
    try {
      while (values.hasNext()) {
        IntWritable value = (IntWritable)values.next();
        LOG.debug("-delete " + index + " doc=" + value);
        reader.deleteDocument(value.get());
      }
    } finally {
      reader.close();
    }
  }

  /** Write nothing. */
  public RecordWriter getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name,
                                      final Progressable progress) throws IOException {
    return new RecordWriter() {                   
        public void write(WritableComparable key, Writable value)
          throws IOException {
          throw new UnsupportedOperationException();
        }        
        public void close(Reporter reporter) throws IOException {}
      };
  }

  public DeleteDuplicates() {
    
  }
  
  public DeleteDuplicates(Configuration conf) {
    setConf(conf);
  }
  
  public void checkOutputSpecs(FileSystem fs, JobConf job) {}

  public void dedup(Path[] indexDirs)
    throws IOException {

    if (LOG.isInfoEnabled()) { LOG.info("Dedup: starting"); }

    Path outDir1 =
      new Path("dedup-urls-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(getConf());

    for (int i = 0; i < indexDirs.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Dedup: adding indexes in: " + indexDirs[i]);
      }
      job.addInputPath(indexDirs[i]);
    }
    job.setJobName("dedup 1: urls by time");

    job.setInputFormat(InputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IndexDoc.class);

    job.setReducerClass(UrlsReducer.class);
    job.setOutputPath(outDir1);

    job.setOutputKeyClass(MD5Hash.class);
    job.setOutputValueClass(IndexDoc.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);
    /*
    Path outDir2 =
      new Path("dedup-hash-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    job = new NutchJob(getConf());
    job.setJobName("dedup 2: content by hash");

    job.addInputPath(outDir1);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(MD5Hash.class);
    job.setMapOutputValueClass(IndexDoc.class);
    job.setPartitionerClass(HashPartitioner.class);
    job.setSpeculativeExecution(false);
    
    job.setReducerClass(HashReducer.class);
    job.setOutputPath(outDir2);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IndexDoc.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    // remove outDir1 - no longer needed
    fs.delete(outDir1);
    */    

    job = new NutchJob(getConf());
    job.setJobName("dedup 3: delete from index(es)");

    job.addInputPath(outDir1);
    job.setInputFormat(SequenceFileInputFormat.class);
    //job.setInputKeyClass(Text.class);
    //job.setInputValueClass(IndexDoc.class);

    job.setInt("io.file.buffer.size", 4096);
    job.setMapperClass(DeleteDuplicates.class);
    job.setReducerClass(DeleteDuplicates.class);

    job.setOutputFormat(DeleteDuplicates.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    JobClient.runJob(job);

    fs.delete(outDir1);

    if (LOG.isInfoEnabled()) { LOG.info("Dedup: done"); }
  }

  public static void main(String[] args) throws Exception {
    int res = new DeleteDuplicates().doMain(NutchConfiguration.create(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length < 1) {
      System.err.println("Usage: DeleteDuplicates <indexes> ...");
      return -1;
    }
    
    Path[] indexes = new Path[args.length];
    for (int i = 0; i < args.length; i++) {
      indexes[i] = new Path(args[i]);
    }
    try {
      dedup(indexes);
      return 0;
    } catch (Exception e) {
      LOG.fatal("DeleteDuplicates: " + StringUtils.stringifyException(e));
      return -1;
    }
  }


  /****************************************************************
  * hash and split urls into keys as is done by the database functions
  ****************************************************************/
  private static MessageDigest digester;
  public synchronized static String getHash( String input ) throws Exception
  {
     if ( digester == null )
        digester = MessageDigest.getInstance("MD5");

     byte[] hashBytes = digester.digest( input.getBytes() );

     StringBuffer hash = new StringBuffer();
     for ( int i=0; i<hashBytes.length; i++ )
     {
        int byteval = hashBytes[i] & 0xFF;
        if ( byteval < 16 )
           hash.append( '0' );
        hash.append( Integer.toHexString( byteval ) );
     }
     return( hash.toString() );
  }

  public static String makeKeysString( String uri )
  {
     String[] uriKeys = k1k2( uri );
     return( uriKeys[0] + ":" + uriKeys[1] );
  }

  public static String k1( String uri )
  {
     return( k1k2( uri )[0] );
  }

  public static String k2( String uri )
  {
     return( k1k2( uri )[1] );
  }

  public static String[] k1k2( String uri )
  {
     String[] keys = new String[2];
     try
     {
        String md5 = getHash( uri );
        keys[0] = "" + new BigInteger( md5.substring(0,16), 16 );
        keys[1] = "" + new BigInteger( md5.substring(16), 16 );
     }
     catch ( Exception e )
     {
        LOG.error( "Can't hash uri: " + uri, e );
     }
     return( keys );
  }


  /****************************************************************
  * Reverse order comparitor for document timestamps.  Documents sorted by this
  * comparitor will have the newest documents first in the list.
  *
  ****************************************************************/
  private static class IndexDocTimeComparator implements Comparator
  {
     public int compare( Object o1, Object o2 )
     {
        IndexDoc this1 = (IndexDoc)o1;
        IndexDoc that1 = (IndexDoc)o2;
        if ( this1.time < that1.time )
           return( 1 );
        else if ( this1.time > that1.time )
           return( -1 );
        return( 0 );
     }
  }
}
