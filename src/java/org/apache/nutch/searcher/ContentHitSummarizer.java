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


import java.io.IOException;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summarizer;
import org.apache.nutch.searcher.HitSummarizer;

/** 
 * extend the HitSummarizer interface, add the lijit-specific getContentSummary method.
 * @author Bill
 *
 */
public interface ContentHitSummarizer extends HitSummarizer {
  
  Summary getContentSummary(HitDetails details, Query query) throws IOException;
  Summary[] getContentSummary(HitDetails[] details, Query query) throws IOException;
}
