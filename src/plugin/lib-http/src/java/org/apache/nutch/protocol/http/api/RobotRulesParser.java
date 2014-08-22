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
package org.apache.nutch.protocol.http.api;

// JDK imports
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.util.RobotsExceptions;


/**
 * This class handles the parsing of <code>robots.txt</code> files.
 * It emits RobotRules objects, which describe the download permissions
 * as described in RobotRulesParser.
 *
 * @author Tom Pierce
 * @author Mike Cafarella
 * @author Doug Cutting
 */
public class RobotRulesParser implements Configurable {
  
  public static final Log LOG = LogFactory.getLog(RobotRulesParser.class);

  private boolean allowForbidden = false;

  private boolean allowPartialAgentMatch = false;

  private static final Hashtable CACHE = new Hashtable();
  
  private static final String CHARACTER_ENCODING= "UTF-8";
  private static final int NO_PRECEDENCE= Integer.MAX_VALUE;
    
  private static final RobotRuleSet EMPTY_RULES= new RobotRuleSet();

  private static RobotRuleSet FORBID_ALL_RULES = getForbidAllRules();

  private Configuration conf;
  private HashMap robotNames;

  /**
   * This class holds the rules which were parsed from a robots.txt
   * file, and can test paths against those rules.
   */
  public static class RobotRuleSet implements RobotRules {
    ArrayList entries = new ArrayList();
    long expireTime;
    long crawlDelay = -1;

    /**
     */
    private class RobotsEntry {
      String prefix;
      boolean allowed;
      boolean ignoreCrawlDelay = false;

      RobotsEntry(String prefix, boolean allowed, boolean ignoreDelay) {
        this.prefix= prefix;
        this.allowed= allowed;
        this.ignoreCrawlDelay = ignoreDelay;
      }
    }

    /**
     */
    private void addPrefix(String prefix, boolean allow) {
      entries.add(new RobotsEntry(prefix, allow, false));
    }

    /**
     * Add an exception path to robots.txt.  These are high priority
     * "allow" paths that override any other "disallow" paths actually
     * in the robots.txt file.  Put them at the front of the tmpEntries
     * ArrayList so they're evaluated first.
     */
    private void addExceptionPrefix( String prefix )
    {
      entries.add( 0, new RobotsEntry( prefix, true, true ) );
    }

    /**
     */
    private void clearPrefixes() {
      entries.clear();
    }

    /**
     * Change when the ruleset goes stale.
     */
    public void setExpireTime(long expireTime) {
      this.expireTime = expireTime;
    }

    /**
     * Get expire time
     */
    public long getExpireTime() {
      return expireTime;
    }

    /**
     * Get Crawl-Delay, in milliseconds. This returns -1 if not set.
     */
    public long getCrawlDelay() {
      return getCrawlDelay( null );
    }

    public long getCrawlDelay(URL url) {
      RobotsEntry entry = getEntry( getPath( url ) );
      if ( entry != null && entry.ignoreCrawlDelay )
        return( 0 );
      return crawlDelay;
    }
    
    /**
     * Set Crawl-Delay, in milliseconds
     */
    public void setCrawlDelay(long crawlDelay) {
      this.crawlDelay = crawlDelay;
    }
    
    /**
     *  Returns <code>false</code> if the <code>robots.txt</code> file
     *  prohibits us from accessing the given <code>url</code>, or
     *  <code>true</code> otherwise.
     */
    public boolean isAllowed(URL url) {
      return isAllowed( getPath( url ) );
    }
    
    /** 
     *  Returns <code>false</code> if the <code>robots.txt</code> file
     *  prohibits us from accessing the given <code>path</code>, or
     *  <code>true</code> otherwise.
     */ 
    public boolean isAllowed(String path) {
      try {
        path= URLDecoder.decode(path, CHARACTER_ENCODING);
      } catch (Exception e) {
        // just ignore it- we can still try to match 
        // path prefixes
      }
      
      RobotsEntry entry = getEntry( path );
      if ( entry != null )
         return( entry.allowed );

      return true;
    }

    private RobotsEntry getEntry( String path )
    {
      Iterator i = entries.iterator();
      while ( i.hasNext() )
      {
        RobotsEntry entry = (RobotsEntry)i.next();
        if ( path.startsWith( entry.prefix ) )
          return( entry );
      }
      return( null );
    }

    /**
     */
    public String toString() {
      isAllowed("x");  // force String[] representation
      StringBuffer buf= new StringBuffer();
      Iterator i = entries.iterator();
      while ( i.hasNext() )
      {
        RobotsEntry entry = (RobotsEntry)i.next();
        if (entry.allowed)
          buf.append("Allow: " + entry.prefix + System.getProperty("line.separator"));
        else 
          buf.append("Disallow: " + entry.prefix + System.getProperty("line.separator"));
      }
      return buf.toString();
    }
  }


  RobotRulesParser() { }

  public RobotRulesParser(Configuration conf) {
    setConf(conf);
  }


  /* ---------------------------------- *
   * <implementation:Configurable> *
   * ---------------------------------- */

  public void setConf(Configuration conf) {
    this.conf = conf;

    allowPartialAgentMatch = conf.getBoolean("http.robots.allowPartialAgentMatch", false);

    allowForbidden = conf.getBoolean("http.robots.403.allow", false);
    //
    // Grab the agent names we advertise to robots files.
    //
    String agentName = conf.get("http.agent.name");
    if (null == agentName) {
      throw new RuntimeException("Agent name not configured!");
    }
    String agentNames = conf.get("http.robots.agents");
    StringTokenizer tok = new StringTokenizer(agentNames, ",");
    ArrayList agents = new ArrayList();
    while (tok.hasMoreTokens()) {
      agents.add(tok.nextToken().trim());
    }

    //
    // If there are no agents for robots-parsing, use our
    // default agent-string.  If both are present, our agent-string
    // should be the first one we advertise to robots-parsing.
    //
    if (agents.size() == 0) {
      agents.add(agentName);
      if (LOG.isFatalEnabled()) {
        LOG.fatal("No agents listed in 'http.robots.agents' property!");
      }
    } else if (!((String)agents.get(0)).equalsIgnoreCase(agentName)) {
      agents.add(0, agentName);
      if (LOG.isFatalEnabled()) {
        LOG.fatal("Agent we advertise (" + agentName
                + ") not listed first in 'http.robots.agents' property!");
      }
    }
    setRobotNames((String[]) agents.toArray(new String[agents.size()]));
  }

  public Configuration getConf() {
    return conf;
  }

  /* ---------------------------------- *
   * <implementation:Configurable> *
   * ---------------------------------- */

  private void setRobotNames(String[] robotNames) {
    this.robotNames= new HashMap();
    for (int i= 0; i < robotNames.length; i++) {
      this.robotNames.put(robotNames[i].toLowerCase(), new Integer(i));
    }
    // always make sure "*" is included
    if (!this.robotNames.containsKey("*"))
      this.robotNames.put("*", new Integer(robotNames.length));
  }

  /**
   *  Creates a new <code>RobotRulesParser</code> which will use the
   *  supplied <code>robotNames</code> when choosing which stanza to
   *  follow in <code>robots.txt</code> files.  Any name in the array
   *  may be matched.  The order of the <code>robotNames</code>
   *  determines the precedence- if many names are matched, only the
   *  rules associated with the robot name having the smallest index
   *  will be used.
   */
  RobotRulesParser(String[] robotNames) {
    setRobotNames(robotNames); 
  }

  /**
   * Returns a {@link RobotRuleSet} object which encapsulates the
   * rules parsed from the supplied <code>robotContent</code>.
   */
  RobotRuleSet parseRules(byte[] robotContent) {
    if (robotContent == null) 
      return EMPTY_RULES;

    String content= new String (robotContent);

    StringTokenizer lineParser= new StringTokenizer(content, "\n\r");

    RobotRuleSet bestRulesSoFar= null;
    int bestPrecedenceSoFar= NO_PRECEDENCE;

    RobotRuleSet currentRules= new RobotRuleSet();
    int currentPrecedence= NO_PRECEDENCE;

    boolean addRules= false;    // in stanza for our robot
    boolean doneAgents= false;  // detect multiple agent lines

    while (lineParser.hasMoreTokens()) {
      String line= lineParser.nextToken();

      // trim out comments and whitespace
      int hashPos= line.indexOf("#");
      if (hashPos >= 0) 
        line= line.substring(0, hashPos);
      line= line.trim();

      if ( (line.length() >= 11) 
           && (line.substring(0, 11).equalsIgnoreCase("User-agent:")) ) {

        if (doneAgents) {
          if (currentPrecedence < bestPrecedenceSoFar) {
            bestPrecedenceSoFar= currentPrecedence;
            bestRulesSoFar= currentRules;
            currentPrecedence= NO_PRECEDENCE;
            currentRules= new RobotRuleSet();
          }
          addRules= false;
        }
        doneAgents= false;

        String agentNames= line.substring(line.indexOf(":") + 1);
        agentNames= agentNames.trim();
        StringTokenizer agentTokenizer= new StringTokenizer(agentNames,",");

        while (agentTokenizer.hasMoreTokens()) {
          // for each agent listed, see if it's us:
          String agentName= agentTokenizer.nextToken().toLowerCase();

          Integer precedenceInt= (Integer) robotNames.get(agentName);

          // If we didn't have an exact match on the agent name, try a fuzzy match (if so configured)
          if ( precedenceInt == null && allowPartialAgentMatch )
            precedenceInt = findMatchingName( agentName );

          if (precedenceInt != null) {
            int precedence= precedenceInt.intValue();
            if ( (precedence < currentPrecedence)
                 && (precedence < bestPrecedenceSoFar) )
              currentPrecedence= precedence;
          }
        }

        if (currentPrecedence < bestPrecedenceSoFar) 
          addRules= true;

      } else if ( (line.length() >= 9)
                  && (line.substring(0, 9).equalsIgnoreCase("Disallow:")) ) {

        doneAgents= true;
        String path= line.substring(line.indexOf(":") + 1);
        path= path.trim();
        try {
          path= URLDecoder.decode(path, CHARACTER_ENCODING);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("error parsing robots rules- can't decode path: " + path);
          }
        }

        if (path.length() == 0) { // "empty rule"
          if (addRules)
            currentRules.clearPrefixes();
        } else {  // rule with path
          if (addRules)
            currentRules.addPrefix(path, false);
        }

      } else if ( (line.length() >= 6)
                  && (line.substring(0, 6).equalsIgnoreCase("Allow:")) ) {

        doneAgents= true;
        String path= line.substring(line.indexOf(":") + 1);
        path= path.trim();

        if (path.length() == 0) { 
          // "empty rule"- treat same as empty disallow
          if (addRules)
            currentRules.clearPrefixes();
        } else {  // rule with path
          if (addRules)
            currentRules.addPrefix(path, true);
        }
      } else if ( (line.length() >= 12)
                  && (line.substring(0, 12).equalsIgnoreCase("Crawl-Delay:"))) {
        doneAgents = true;
        if ( addRules )
        {
          long crawlDelay = -1;
          String delay = line.substring("Crawl-Delay:".length(), line.length()).trim();
          if (delay.length() > 0) {
            try {
              crawlDelay = Long.parseLong(delay) * 1000; // sec to millisec
            } catch (Exception e) {
              LOG.info("can not parse Crawl-Delay:" + e.toString());
            }
            currentRules.setCrawlDelay(crawlDelay);
          }
        }
      }
    }

    if (currentPrecedence < bestPrecedenceSoFar) {
      bestPrecedenceSoFar= currentPrecedence;
      bestRulesSoFar= currentRules;
    }

    if (bestPrecedenceSoFar == NO_PRECEDENCE) 
      return EMPTY_RULES;
    return bestRulesSoFar;
  }
  
  /****************************************************************
  * Find an entry in the robot names array (the names we go by) that
  * matches the agent name we're currently working in the fetched
  * robots.txt file.
  *
  * @param agentName  The string from the fetched robots.txt file
  *
  ****************************************************************/
  private Integer findMatchingName( String agentName )
  {  
     Iterator i = robotNames.keySet().iterator();
     while ( i.hasNext() )
     {
        String key = (String)i.next();
        if ( key.indexOf(agentName) != -1 )
           return( (Integer)robotNames.get( key ) );
     }
     return( null );
  }
  
  /****************************************************************
  *
  ****************************************************************/
  static String getPath( URL url )
  {
     String path = url.getPath();                  // check rules
     if ((path == null) || "".equals(path)) {
       path= "/";
     }
     return( path );
  }

  /**
   *  Returns a <code>RobotRuleSet</code> object appropriate for use
   *  when the <code>robots.txt</code> file is empty or missing; all
   *  requests are allowed.
   */
  static RobotRuleSet getEmptyRules() {
    return EMPTY_RULES;
  }

  /**
   *  Returns a <code>RobotRuleSet</code> object appropriate for use
   *  when the <code>robots.txt</code> file is not fetched due to a
   *  <code>403/Forbidden</code> response; all requests are
   *  disallowed.
   */
  static RobotRuleSet getForbidAllRules() {
    RobotRuleSet rules= new RobotRuleSet();
    rules.addPrefix("", false);
    return rules;
  }
  
  public RobotRuleSet getRobotRulesSet(HttpBase http, Text url) {
    URL u = null;
    try {
      u = new URL(url.toString());
    } catch (Exception e) {
      return EMPTY_RULES;
    }
    return getRobotRulesSet(http, u);
  }
  
  private RobotRuleSet getRobotRulesSet(HttpBase http, URL url) {

    String host = url.getHost().toLowerCase(); // normalize to lower case

    RobotRuleSet robotRules = (RobotRuleSet)CACHE.get(host);

    boolean cacheRule = true;
    
    if (robotRules == null) {                     // cache miss
      if (LOG.isTraceEnabled()) { LOG.trace("cache miss " + url); }
      try {
        Response response = http.getResponse(new URL(url, "/robots.txt"),
                                             new CrawlDatum(), true);

        if (response.getCode() == 200)               // found rules: parse them
          robotRules = parseRules(response.getContent());
        else if ( (response.getCode() == 403) && (!allowForbidden) )
          robotRules = FORBID_ALL_RULES;            // use forbid all
        else if (response.getCode() >= 500) {
          cacheRule = false;
          robotRules = EMPTY_RULES;
        }else                                        
          robotRules = EMPTY_RULES;                 // use default rules
      } catch (Throwable t) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Couldn't get robots.txt for " + url + ": " + t.toString());
        }
        cacheRule = false;
        robotRules = EMPTY_RULES;
      }

      addAllowExceptions( host, robotRules );

      if (cacheRule){
        CACHE.put(host, robotRules);  // cache rules for host
      }
    }
    return robotRules;
  }

  /****************************************************************
  *
  ****************************************************************/
  protected RobotRuleSet addAllowExceptions( String host, RobotRuleSet robotRules )
  {
     boolean rulesModified = false;

     // Empty rules already permit all, so we don't need to do anything with them
     if ( robotRules != EMPTY_RULES )
     {
        // Hosts are not case-sensitive, paths are.
        host = host.toLowerCase();
        LOG.debug( "Adding ignore rules for " + host );

        // Overly pedantic way of doing this, but trying to make it clear what's going on
        String wwwHost = host;
        String nowwwHost = host;
        if ( host.startsWith("www") )
           nowwwHost = host.substring( 4 );
        else
           wwwHost = "www." + host;
        LOG.debug( "wwwHost " + wwwHost );
        LOG.debug( "nowwwHost " + nowwwHost );

        // If one of the "ignore robots.txt" urls matches this host, either with
        // or without the "www", add an explicit "allow" rule.
        // Exception URLs must not have the protocol, and must have the host.
        String[] paths = RobotsExceptions.getRobotsExceptions( conf );
        for ( String path : paths )
        {
           try
           {
              LOG.debug( "Ignore path: " + path );

              // Get the path's host.  If there's no /, it's just the host, so add a slash to
              // create an empty path.
              int ndx = path.indexOf( "/" );
              if ( ndx < 0 )
              {
                 path = path + "/";
                 ndx = path.indexOf( "/" );
              }
              String pathHost = path.substring( 0, ndx ).toLowerCase();
              path = path.substring( ndx );

              // Check the path against the host
              if ( pathHost.equals( wwwHost ) || pathHost.equals( nowwwHost ) )
              {
                 // If the rules are the static "forbid all" instance, get a new instance so
                 // we don't affect the forbid all rules for all other hosts.
                 if ( robotRules == FORBID_ALL_RULES )
                 {
                    LOG.info( "Getting new instance of 'forbid all' rule set" );
                    robotRules = getForbidAllRules();
                 }

                 // Add a "permit" rule for this host and path
                 LOG.info( "Adding allow exception rule for " + host + ": " + path );
                 robotRules.addExceptionPrefix( path );
                 rulesModified = true;
              }
           }
           catch ( Exception e )
           {
              LOG.error( "Exception loading rules exceptions", e );
           }
        }
     }

     if ( rulesModified )
        LOG.info( "Robot rules for " + host + " after allow exceptions:\n" + robotRules );

     return( robotRules );
  }

  public boolean isAllowed(HttpBase http, URL url)
      throws ProtocolException, IOException {
    String path = getPath( url );
    return getRobotRulesSet(http, url).isAllowed(path);
  }
  
  public long getCrawlDelay(HttpBase http, URL url)
      throws ProtocolException, IOException {
    return getRobotRulesSet(http, url).getCrawlDelay(url);
  }

  private final static int BUFSIZE= 2048;

  /** command-line main for testing */
  public static void main(String[] argv) {
    if (argv.length < 3) {
      System.out.println("Usage:");
      System.out.println("   java <robots-file> <url-file> <agent-name>+");
      System.out.println("");
      System.out.println("The <robots-file> will be parsed as a robots.txt file,");
      System.out.println("using the given <agent-name> to select rules.  URLs ");
      System.out.println("will be read (one per line) from <url-file>, and tested");
      System.out.println("against the rules.");
      System.exit(-1);
    }
    try { 
      FileInputStream robotsIn= new FileInputStream(argv[0]);
      LineNumberReader testsIn= new LineNumberReader(new FileReader(argv[1]));
      String[] robotNames= new String[argv.length-2];

      for (int i=0; i < robotNames.length; i++)
      {
        robotNames[i]= argv[i+2];
        System.out.println( "Testing robot name: " + robotNames[i] );
      }
      System.out.println();

      ArrayList bufs= new ArrayList();
      byte[] buf= new byte[BUFSIZE];
      int totBytes= 0;

      int rsize= robotsIn.read(buf);
      while (rsize >= 0) {
        totBytes+= rsize;
        if (rsize != BUFSIZE) {
          byte[] tmp= new byte[rsize];
          System.arraycopy(buf, 0, tmp, 0, rsize);
          bufs.add(tmp);
        } else {
          bufs.add(buf);
          buf= new byte[BUFSIZE];
        }
        rsize= robotsIn.read(buf);
      }

      byte[] robotsBytes= new byte[totBytes];
      int pos= 0;

      for (int i= 0; i < bufs.size(); i++) {
        byte[] currBuf= (byte[]) bufs.get(i);
        int currBufLen= currBuf.length;
        System.arraycopy(currBuf, 0, robotsBytes, pos, currBufLen);
        pos+= currBufLen;
      }

      //System.out.println( "Robots file contents:\n" + new String(robotsBytes) );
      RobotRulesParser parser= 
        new RobotRulesParser(robotNames);
      RobotRuleSet rules= parser.parseRules(robotsBytes);
      System.out.println("Rules:");
      System.out.println(rules);
      System.out.println();

      String testPath= testsIn.readLine().trim();
      while (testPath != null) {
        try
        {
          URL url = new URL( testPath );
          System.out.println( (rules.isAllowed(url) ? 
                               "allowed" : "not allowed")
                              + ":\t" + url);
        }
        catch ( Exception e )
        {
          System.out.println( (rules.isAllowed(testPath) ? 
                               "allowed" : "not allowed")
                              + ":\t" + testPath);
        }
        testPath= testsIn.readLine();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
