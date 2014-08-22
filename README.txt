These patches (so far) apply only to crawling, and not to searching, and
do not involve any Nutch plugins, so including the resulting jar file
in the classpath before the Nutch jars will get these patches executed.

You should make sure that the Nutch webapp has already been deployed
into the apache webapps directory before building these patches.  There
are a couple files copied from here to there, and if the real Nutch
webapp gets deployed after this, these files will get overwritten.
