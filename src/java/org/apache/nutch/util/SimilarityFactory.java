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

package org.apache.nutch.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.lucene.search.Similarity;

import org.apache.hadoop.conf.Configuration;

/****************************************************************
*
****************************************************************/
public class SimilarityFactory {
   public static final Log LOG = LogFactory.getLog( SimilarityFactory.class );

   /***********************************************************
   * Create an instance of the similarity class that is configured by name.
   *
   * @exception Any instantiation exception is converted to an IOException
   ***********************************************************/
   public static Similarity getSimilarity( String className ) throws IOException {
      try {
         Class similarityClass = Class.forName( className );
         Similarity similarity = (Similarity)similarityClass.newInstance();
         if ( LOG.isDebugEnabled() )
            LOG.debug( "Instantiated new similarity: " + similarity );
         return( similarity );
      }
      catch ( LinkageError err ) {
         // Convert LinkageError into IOException
         throw new IOException( "Can't create similarity class due to " + err );
      }
      catch ( Exception ex ) {
         // Convert Exception into IOException
         throw new IOException( "Can't create similarity class due to " + ex );
      }
   }

   /***********************************************************
   * Create an instance of the similarity class that is configured by name.
   *
   * @exception Any instantiation exception is converted to an IOException
   ***********************************************************/
   public static Similarity getSimilarity( Configuration conf, String propertyName, String defaultClassname ) throws IOException {
      String className = conf.get( propertyName, defaultClassname );
      return( getSimilarity( className ) );
   }
}

