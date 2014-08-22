//==========================================================================//
//          Copyright Lijit Networks, Inc. 2008 All Rights Reserved         //
//                                                                          //
//                 PROPRIETARY AND CONFIDENTIAL INFORMATION                 //
// The information contained herein is the proprietary and confidential     //
// property of Lijit Networks, Inc. and may not be used, distributed,       //
// modified, disclosed or reproduced without the express written            //
// permission of Lijit Networks, Inc.                                       //
//==========================================================================//
//
// Lijit specific enhancement to the org.apache.nutch.Summarizer interface.
//
//==========================================================================//
package org.apache.nutch.searcher;

// Nutch imports
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.Summarizer;


/** 
 * Extend the summarizer class, add our getContentSummary method.
 *
 */
public interface ContentSummarizer extends Summarizer {

  public Summary getContentSummary(String text, Query query);
}
