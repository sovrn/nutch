//==========================================================================//
//          Copyright Lijit Networks, Inc. 2009 All Rights Reserved         //
//                                                                          //
//                 PROPRIETARY AND CONFIDENTIAL INFORMATION                 //
// The information contained herein is the proprietary and confidential     //
// property of Lijit Networks, Inc. and may not be used, distributed,       //
// modified, disclosed or reproduced without the express written            //
// permission of Lijit Networks, Inc.                                       //
//==========================================================================//
package org.apache.nutch.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

/****************************************************************
*
****************************************************************/
public class RobotsExceptions
{
   public static final Log log = LogFactory.getLog( RobotsExceptions.class );

   /****************************************************************
   * Store robots exceptions in the Hadoop configuration object.
   *  The crawler passes exceptions to robots.txt disallow rules to the
   *  Nutch HTTP protocol lib plugin via the Nutch configuration object.
   *  This is the code that sets those exceptions.
   ****************************************************************/
   public static void setRobotsExceptions( Configuration conf, String[] exceptionPaths )
   {
      log.info( "Adding robots exceptions..." );

      conf.set( "http.robots.exception.count", exceptionPaths.length );
      for ( int i=0; i<exceptionPaths.length; i++ )
      {
         String path = exceptionPaths[i];

         // Cut the protocol off the path if it's there at/near the beginning
         int ndx = path.indexOf( "://" );
         if ( ndx >= 0 && ndx <= 6 )
            path = path.substring( ndx + 3 );
         log.info( "Ignore path: " + path );

         // Set the path exception
         conf.set( "http.robots.exception." + i, path );
      }
   }

   /****************************************************************
   * Get the robots exceptions from the Hadoop configuration object.
   *   The Crawler passes exceptions to robots.txt disallow rules via
   *   the crawl Configuration object with code in CrawlUtil.  This
   *   method reads those exceptions from the configuration.
   *   If, for some reason, the config *is* hosed, use a bogus path.
   ****************************************************************/
   public static String[] getRobotsExceptions( Configuration conf )
   {
      int count = conf.getInt( "http.robots.exception.count", 0 );
      String[] paths = new String[count];
      for ( int i=0; i<count; i++ )
         paths[i] = conf.get( "http.robots.exception." + i, "this-host-string-is-bogus" );
      return( paths );
   }
   
   /****************************************************************
   *
   ****************************************************************/
   public static boolean okToIgnore( Configuration conf, String url )
   {
      return( okToIgnore( getRobotsExceptions( conf ), url ) );
   }

   /****************************************************************
   *
   ****************************************************************/
   public static boolean okToIgnore( String[] exceptions, String url )
   {
      // Strip the protocol if it's on the url
      int ndx = url.indexOf( "://" );
      if ( ndx > 0 )
         url = url.substring( ndx + 3 );

      // Also strip www if it's there.
      if ( url.startsWith( "www." ) )
         url = url.substring( 4 );

      // Now compare to all exceptions
      for ( String exception : exceptions )
      {
         if ( exception.startsWith( "www." ) )
            exception = exception.substring( 4 );

         // Just return early if we find a match
         if ( url.startsWith(exception) )
            return( true );
      }
      return( false );
   }
}

