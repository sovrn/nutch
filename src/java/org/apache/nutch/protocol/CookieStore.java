//==========================================================================//
//          Copyright Lijit Networks, Inc. 2009 All Rights Reserved         //
//                                                                          //
//                 PROPRIETARY AND CONFIDENTIAL INFORMATION                 //
// The information contained herein is the proprietary and confidential     //
// property of Lijit Networks, Inc. and may not be used, distributed,       //
// modified, disclosed or reproduced without the express written            //
// permission of Lijit Networks, Inc.                                       //
//==========================================================================//
package org.apache.nutch.protocol;

import java.net.URL;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.ArrayList;
import java.text.SimpleDateFormat;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/****************************************************************
*
* I was gonna call it CookieJar, but that was just too precious.
*
* This file is identical (except for the package) to CookieStore
* in com.lijit.util.
*
****************************************************************/
public class CookieStore
{
   private final static Log log = LogFactory.getLog( CookieStore.class );

   private static HashMap savedCookies = new HashMap();

   /****************************************************************
   *
   ****************************************************************/
   public static void saveCookie( String url, String cookieStr )
   {
      try
      {
         saveCookie( new URL(url), cookieStr );
      }
      catch ( Exception e )
      {
         log.error( "Can't save cookie for " + url, e );
      }
   }

   /****************************************************************
   *
   ****************************************************************/
   public static void saveCookie( URL url, String cookieStr )
   {
      try
      {
         HttpCookie cookie = parseCookie( url, cookieStr );
         saveCookie( cookie );
         log.debug( "Saved cookie for " + url + ": " + cookie );
      }
      catch ( Exception e )
      {
         log.error( "Failed to parse/store cookie: " + url + " / " + cookieStr );
      }
   }

   /****************************************************************
   *
   ****************************************************************/
   public static void saveCookie( HttpCookie cookie )
   {
      String domain = cookie.getDomain();

      // Get the table of cookies for that domain
      Hashtable hostCookies = null;
      synchronized( savedCookies )
      {
         hostCookies = (Hashtable)savedCookies.get( domain );
         if ( hostCookies == null )
         {
            hostCookies = new Hashtable();
            savedCookies.put( domain, hostCookies );
         }
      }

      hostCookies.put( cookie.getName(), cookie );
   }

   /****************************************************************
   *
   ****************************************************************/
   public static String[] getCookies( String url )
   {
      try
      {
         return( getCookies( new URL(url) ) );
      }
      catch ( Exception e )
      {
         log.error( "Can't get cookies for " + url, e );
      }
      return( null );
   }
   public static String[] getCookies( URL url )
   {
      String[] result = null;

      // Bust up the URL
      String host = url.getHost();
      String path = url.getPath();

      log.debug( "Get cookies for " + url );
      ArrayList cookies = new ArrayList();
      synchronized( savedCookies )
      {
         // Have to look at all the cookies for all the domains we have stored
         Iterator i1 = savedCookies.keySet().iterator();
         while ( i1.hasNext() )
         {
            // Does this domain match the host portion of the URL?
            // Simplistic test; may match too broadly.
            String domain = (String)i1.next();
            if ( host.endsWith(domain) ) 
            {
               log.debug( "We have cookies from " + domain );
               Hashtable hostCookies = (Hashtable)savedCookies.get( domain );
               Iterator i2 = hostCookies.values().iterator();
               while ( i2.hasNext() )
               {
                  // Check the expiration and path of the cookie
                  HttpCookie cookie = (HttpCookie)i2.next();
                  if ( cookie.hasExpired() )
                     i2.remove();           // it's expired; toss yer cookie
                  else if ( path.startsWith( cookie.getPath() ) )
                     cookies.add( cookie );
               }
            }
         }
      }

      // If we got some cookies, convert them into an array of key=value strings
      if ( cookies.size() != 0 )
      {
         result = new String[ cookies.size() ];
         for ( int i=0; i<result.length; i++ )
         {
            HttpCookie cookie = (HttpCookie)cookies.get(i);
            result[i] = cookie.getName() + "=" + cookie.getValue();
            log.debug( "Sending cookie to " + host + ": " + result[i] );
         }
      }

      return( result );
   }

   /****************************************************************
   * Parse the fields of a cookie (path, expiration, etc.) from its value.
   * If the domain and path fields are missing, use the values from the URL.
   *
   ****************************************************************/
   public static HttpCookie parseCookie( String url, String cookieStr ) throws MalformedURLException
   {
      return( parseCookie( new URL(url), cookieStr ) );
   }

   public static HttpCookie parseCookie( URL url, String cookieStr )
   {
      HttpCookie cookie = null;

      // Split the cookie string around semicolon-delimited fields
      String[] fields = cookieStr.split(";");

      // Parse each field of the cookie
      for ( int i=0; i<fields.length; i++ )
      {
         // Bust the cookie field apart around the '='
         String[] field = fields[i].split("=");
         String fieldName = field[0].trim();
         String fieldValue = (field.length>1) ? field[1] : "";

         // Cookie name and value pair is always the first cookie field
         if ( i == 0 )
         {
            cookie = new HttpCookie( fieldName, fieldValue );
            cookie.setDomain( url.getHost() );
            cookie.setPath( url.getPath() );
         }
         else if ( "secure".equalsIgnoreCase( fieldName ) )
            cookie.setSecure( true );
         else if ( "expires".equalsIgnoreCase( fieldName ) )
            cookie.setMaxAge( parseCookieMaxAge( fieldValue ) );
         else if ( "domain".equalsIgnoreCase( fieldName ) && fieldValue.length() > 0 )
            cookie.setDomain( fieldValue );
         else if ( "path".equalsIgnoreCase( fieldName ) && fieldValue.length() > 0 )
            cookie.setPath( fieldValue );
      }

      return( cookie );
   }

   /****************************************************************
   * Try to parse the expiry time from a cookie value string
   *
   ****************************************************************/
   public static Date parseCookieExpires( String dateStr )
   {
      // cookie expiry time looks like this: Wdy, DD-Mon-YYYY HH:MM:SS GMT
      // Ignore the weekday thing
      dateStr = dateStr.substring( dateStr.indexOf(',') + 1 ).trim();

      // Try to parse out the date string with the correct format
      Date expiryDate = null;
      try
      {
         try
         {
            SimpleDateFormat fmt = new SimpleDateFormat( "dd-MMM-yyyy HH:mm:ss zzz" );
            expiryDate = fmt.parse( dateStr );
         }
         catch ( Exception e )
         {
            // Sometimes the date is right, sometimes it looks like this: Wdy, DD Mon YYYY HH:MM:SS GMT
            SimpleDateFormat fmt = new SimpleDateFormat( "dd MMM yyyy HH:mm:ss zzz" );
            expiryDate = fmt.parse( dateStr );
         }
      }
      catch ( Exception e )
      {
         // I'm sure there are other ways servers screw up cookie expiration timestamps
         //System.out.println( "Cookie expiration not in proper format" );
      }

      return( expiryDate );
   }

   /****************************************************************
   * Try to parse the max cookie age from a cookie value string
   *
   ****************************************************************/
   public static long parseCookieMaxAge( String dateStr )
   {
      // Try to parse out the date string with the correct format
      Date expiryDate = parseCookieExpires( dateStr );

      // If we got a good date, convert it to max age seconds
      long maxAge = -1;
      if ( expiryDate != null )
         maxAge = ( expiryDate.getTime() - System.currentTimeMillis() ) / 1000;

      return( maxAge );
   }
}
