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
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolBase;
import org.apache.nutch.parse.*;
import org.apache.nutch.analysis.*;

import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.SimilarityFactory;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDb;

import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;

/** Create indexes for segments. */
public class Indexer extends ToolBase implements Reducer, Mapper {
  
  public static final String DONE_NAME = "index.done";

  public static final Log LOG = LogFactory.getLog(Indexer.class);

  // LIJIT: make the similarity class configurable:
  protected static String similarityClassName = NutchSimilarity.class.getName();
  // /LIJIT

  /** Unwrap Lucene Documents created by reduce and add them to an index. */
  public static class OutputFormat
    extends org.apache.hadoop.mapred.OutputFormatBase {
    public RecordWriter getRecordWriter(final FileSystem fs, JobConf job,
                                        String name, Progressable progress) throws IOException {
      final Path perm = new Path(job.getOutputPath(), name);
      final Path temp =
        job.getLocalPath("index/_"+Integer.toString(new Random().nextInt()));

      fs.delete(perm);                            // delete old, if any

      final AnalyzerFactory factory = new AnalyzerFactory(job);
      final IndexWriter writer =                  // build locally first
        new IndexWriter(fs.startLocalOutput(perm, temp).toString(),
                        new NutchDocumentAnalyzer(job), true);


      writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
      writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
      writer.setMaxMergeDocs(job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));
      writer.setTermIndexInterval
        (job.getInt("indexer.termIndexInterval", 128));
      writer.setMaxFieldLength(job.getInt("indexer.max.tokens", 10000));
      writer.setInfoStream(LogUtil.getInfoStream(LOG));
      writer.setUseCompoundFile(false);
      
      // LIJIT: make the similarity class configurable:
      writer.setSimilarity( SimilarityFactory.getSimilarity( similarityClassName ) );
      // /LIJIT

      return new RecordWriter() {
          boolean closed;

          public void write(WritableComparable key, Writable value)
            throws IOException {                  // unwrap & index doc
            Document doc = (Document)((ObjectWritable)value).get();
            NutchAnalyzer analyzer = factory.get(doc.get("lang"));
            if (LOG.isInfoEnabled()) {
              LOG.info(" Indexing [" + doc.getField("url").stringValue() + "]" +
                       " with analyzer " + analyzer +
                       " (" + doc.get("lang") + ")");
            }
            writer.addDocument(doc, analyzer);
          }
          
          public void close(final Reporter reporter) throws IOException {
            // spawn a thread to give progress heartbeats
            Thread prog = new Thread() {
                public void run() {
                  while (!closed) {
                    try {
                      reporter.setStatus("closing");
                      Thread.sleep(1000);
                    } catch (InterruptedException e) { continue; }
                      catch (Throwable e) { return; }
                  }
                }
              };

            try {
              prog.start();
              if (LOG.isInfoEnabled()) { LOG.info("Optimizing index."); }
              // optimize & close index
              writer.optimize();
              writer.close();
              fs.completeLocalOutput(perm, temp);   // copy to dfs
              fs.createNewFile(new Path(perm, DONE_NAME));
            } finally {
              closed = true;
            }
          }
        };
    }
  }

  private IndexingFilters filters;
  private ScoringFilters scfilters;

  public Indexer() {
    
  }
  
  public Indexer(Configuration conf) {
    setConf(conf);
    // LIJIT: make the similarity class configurable:
    this.similarityClassName = conf.get( "indexer.similarityclass", similarityClassName );
    // /LIJIT
  }
  
  public void configure(JobConf job) {
    setConf(job);
    this.filters = new IndexingFilters(getConf());
    this.scfilters = new ScoringFilters(getConf());
    // LIJIT: make the similarity class configurable:
    this.similarityClassName = conf.get( "indexer.similarityclass", similarityClassName );
    // /LIJIT
  }

  public void close() {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    LOG.debug( "---------------------------------------------------------------------------------------------------" );
    LOG.debug( "REDUCE: " + key );

    Inlinks inlinks = null;
    CrawlDatum dbDatum = null;
    CrawlDatum fetchDatum = null;
    ParseData parseData = null;
    ParseText parseText = null;
    while (values.hasNext()) {
      Object value = ((ObjectWritable)values.next()).get(); // unwrap
      if (value instanceof Inlinks) {
        inlinks = (Inlinks)value;
      } else if (value instanceof CrawlDatum) {
        CrawlDatum datum = (CrawlDatum)value;
        if (CrawlDatum.hasDbStatus(datum))
        {
          dbDatum = datum;
          if ( LOG.isDebugEnabled() ) LOG.debug("hasDbStatus: " + key + "\n" + datum ); // LIJIT
        }
        else if (CrawlDatum.hasFetchStatus(datum))
        {
          fetchDatum = datum;
          if ( LOG.isDebugEnabled() ) LOG.debug("hasFetchStatus: " + key + "\n" + datum ); // LIJIT
        }
        /* LIJIT: don't freak out if this has linked status in the crawl datum...
                  It's not really a redirect.                                           */
        else if ( CrawlDatum.STATUS_LINKED == datum.getStatus() ||
                  CrawlDatum.STATUS_SIGNATURE == datum.getStatus() )
        {
          if ( LOG.isDebugEnabled() ) LOG.debug( "skipping linked/signature crawl datum" );
          continue;
        }
        /* LIJIT end */
        else
        {
          throw new RuntimeException("Unexpected status: "+datum.getStatus());
        }
      } else if (value instanceof ParseData) {
        parseData = (ParseData)value;
        if ( LOG.isDebugEnabled() ) LOG.debug( "parse data: " + parseData );   // LIJIT
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
        if ( LOG.isDebugEnabled() ) LOG.debug( "parse text: " + parseText );   // LIJIT
      } else if (LOG.isWarnEnabled()) {
        LOG.warn("Unrecognized type: "+value.getClass());
      }
    }      

    /* LIJIT:
       For some reason, Nutch has the habit of fetching the same page multiple times.  It's
       not clear why this happens, but it could cause the crawler to go on and on fetching
       pages it already has.  Soooo....
       We keep track of the fetched pages in memory, and at the URLFilter step will filter
       out already fetched pages.
       This has the problem, though, of occasionally causing Nutch to discard the fetchDatum
       it already had for the page (must have something to do with multiple cycles of map/reduce
       while crawling).  So in such cases, we'll have everything but a fetchDatum.
       If we don't have the fetch datum, but have everything else and a crawl db
       status of "fetched", simulate a fetch datum by cloning the db datum  */
    if ( fetchDatum == null && parseText != null && parseData != null &&
         dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_FETCHED )
    {
       fetchDatum = (CrawlDatum)dbDatum.clone();
       fetchDatum.setStatus( CrawlDatum.STATUS_FETCH_SUCCESS );
    }

    // When reindexing we almost always are missing crawl db and fetch CrawlDatum objects
    if ( fetchDatum == null )
       fetchDatum = new CrawlDatum( CrawlDatum.STATUS_FETCH_SUCCESS, 365L );
    if ( dbDatum == null )
       dbDatum = new CrawlDatum( CrawlDatum.STATUS_FETCH_SUCCESS, 365L );
    /* end LIJIT */

    if (fetchDatum == null || dbDatum == null
        || parseText == null || parseData == null) {
      /* LIJIT:  Extra logging  */
      LOG.debug( "Indexer: skipping document: " + key );
      if ( fetchDatum == null ) LOG.debug( "Indexer: null fetchDatum" );
      if ( dbDatum == null ) LOG.debug( "Indexer: null dbDatum" );
      if ( parseText == null ) LOG.debug( "Indexer: null parseText" );
      if ( parseData == null ) LOG.debug( "Indexer: null parseData" );
      /* end lijit */
      return;                                     // only have inlinks
    }

    Document doc = new Document();
    Metadata metadata = parseData.getContentMeta();

    // add segment, used to map from merged index back to segment files
    /* LIJIT: index the segment field untokenized   */
    LOG.debug( "Indexer: indexing the segment field");
    doc.add(new Field("segment", metadata.get(Nutch.SEGMENT_NAME_KEY),
            Field.Store.YES, Field.Index.UN_TOKENIZED));

    // add digest, used by dedup
    doc.add(new Field("digest", metadata.get(Nutch.SIGNATURE_KEY),
            Field.Store.YES, Field.Index.NO));

//     if (LOG.isInfoEnabled()) {
//       LOG.info("Url: "+key.toString());
//       LOG.info("Title: "+parseData.getTitle());
//       LOG.info(crawlDatum.toString());
//       if (inlinks != null) {
//         LOG.info(inlinks.toString());
//       }
//     }

    Parse parse = new ParseImpl(parseText, parseData);
    try {
      // run indexing filters
      doc = this.filters.filter(doc, parse, (Text)key, fetchDatum, inlinks);
    } catch (IndexingException e) {
      if (LOG.isWarnEnabled()) { LOG.warn("Error indexing "+key+": "+e); }
      return;
    }

    float boost = 1.0f;
    // run scoring filters
    // LIJIT: skip scoring of documents; because we don't have a full
    // page link graph (incomplete page rank), make all documents score 1.0
    /*
    try {
      boost = this.scfilters.indexerScore((Text)key, doc, dbDatum,
              fetchDatum, parse, inlinks, boost);
    } catch (ScoringFilterException e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Error calculating score " + key + ": " + e);
      }
      return;
    }
    */
    /*  END LIJIT  */
    // apply boost to all indexed fields.
    doc.setBoost(boost);
    // store boost for use by explain and dedup
    doc.add(new Field("boost", Float.toString(boost),
            Field.Store.YES, Field.Index.NO));

    output.collect(key, new ObjectWritable(doc));
  }

  public void index(Path indexDir, Path crawlDb, Path linkDb, Path[] segments)
    throws IOException {

    FileSystem fs = FileSystem.get( conf );

    if (LOG.isInfoEnabled()) {
      LOG.info("Indexer: starting");
      LOG.info("Indexer: linkdb: " + linkDb);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("index " + indexDir);

    for (int i = 0; i < segments.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Indexer: adding segment: " + segments[i]);
      }

      // When reindexing, we probably don't have a fetch datum DB
      Path fetchDB = new Path( segments[i], CrawlDatum.FETCH_DIR_NAME );
      if ( fs.exists( fetchDB ) )
         job.addInputPath(new Path(segments[i], CrawlDatum.FETCH_DIR_NAME));
      else
         LOG.info( "no fetch dir" );

      job.addInputPath(new Path(segments[i], ParseData.DIR_NAME));
      job.addInputPath(new Path(segments[i], ParseText.DIR_NAME));
    }

    // LIJIT:
    // When reindexing, we don't have a crawl DB or link DB
    if ( crawlDb != null )
       job.addInputPath(new Path(crawlDb, CrawlDb.CURRENT_NAME));
    if ( linkDb != null )
       job.addInputPath(new Path(linkDb, LinkDb.CURRENT_NAME));
    // /LIJIT
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(Indexer.class);
    job.setReducerClass(Indexer.class);

    job.setOutputPath(indexDir);
    job.setOutputFormat(OutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ObjectWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Indexer: done"); }
  }

  public static void main(String[] args) throws Exception {
    int res = new Indexer().doMain(NutchConfiguration.create(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length < 4) {
      System.err.println("Usage: <index> <crawldb> <linkdb> <segment> ...");
      return -1;
    }
    
    Path[] segments = new Path[args.length-3];
    for (int i = 3; i < args.length; i++) {
      segments[i-3] = new Path(args[i]);
    }

    try {
      index(new Path(args[0]), new Path(args[1]), new Path(args[2]),
                  segments);
      return 0;
    } catch (Exception e) {
      LOG.fatal("Indexer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public void map(WritableComparable key, Writable value,
      OutputCollector output, Reporter reporter) throws IOException {
    output.collect(key, new ObjectWritable(value));
  }

}
