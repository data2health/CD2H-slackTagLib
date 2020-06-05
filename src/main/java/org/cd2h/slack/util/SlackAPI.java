package org.cd2h.slack.util;

import edu.uiowa.extraction.LocalProperties;
import edu.uiowa.extraction.PropertyLoader;

public class SlackAPI {
    static LocalProperties prop_file = null;
    static String OAuthToken = null;
    
    static {
	prop_file = PropertyLoader.loadProperties("slack");
	OAuthToken = prop_file.getProperty("OAuth");
    }
    
    static String getOAuthToken() {
	return OAuthToken;
    }
}
