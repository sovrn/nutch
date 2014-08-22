<%--
         Copyright Lijit Networks, Inc. 2009 All Rights Reserved     
                                                                     
                PROPRIETARY AND CONFIDENTIAL INFORMATION             
The information contained herein is the proprietary and confidential 
property of Lijit Networks, Inc. and may not be used, distributed,   
modified, disclosed or reproduced without the express written        
permission of Lijit Networks, Inc.                                   
--%>
<%@ page 
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
  import="com.lijit.nutch.reportscrawler.LijitAPIServlet"
%>
<html>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<head>
<title>Prospecting tool</title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon"/>
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon"/>
<jsp:include page="include/style.html"/>
<script type="text/javascript">
<!--
function queryfocus() { document.prospect.query.focus(); }
// -->
</script>
</head>

<body onLoad="queryfocus();">

<jsp:include page="/en/include/header.html"/>
<br>
 <form name="prospect" action="/lijitapi" method="get"/>
   <input type="hidden" name="<%=LijitAPIServlet.FUNCTION%>" value="<%=LijitAPIServlet.GET_PROSPECTS_FOR_TERM%>"/>

   Term: <input type="text" name="<%=LijitAPIServlet.TERM%>" size=44 value=""/><br/>
   Term: <input type="text" name="<%=LijitAPIServlet.TERM%>" size=44 value=""/><br/>
   Term: <input type="text" name="<%=LijitAPIServlet.TERM%>" size=44 value=""/><br/>
   Term: <input type="text" name="<%=LijitAPIServlet.TERM%>" size=44 value=""/><br/>

   Blog count: <input type="text" name="<%=LijitAPIServlet.TOP_BLOG_COUNT%>" value="10"/><br/>

   Min hits per blog: <input type="text" name="<%=LijitAPIServlet.MIN_BLOG_HITS%>" value="10"/><br/>

   Output: XML <input type="radio" name="<%=LijitAPIServlet.OUTPUT_FORMAT%>" value="<%=LijitAPIServlet.XML_OUTPUT%>"/> Text <input type="radio" name="outputformat" value="<%=LijitAPIServlet.TEXT_OUTPUT%>" checked/><br/>

   <input type="submit" value="go"><br>
 </form>

</body>
</html>
