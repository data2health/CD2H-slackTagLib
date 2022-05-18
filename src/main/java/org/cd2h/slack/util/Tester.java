package org.cd2h.slack.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;
import org.json.JSONTokener;

public class Tester extends SlackAPI {
	static Logger logger = LogManager.getLogger(Tester.class);
    
    static public void main(String[] args) {
	String prefix = "https://slack.com/api/conversations.list?token=";
	String suffix = "&pretty=1";
	fetch(prefix, suffix);
    }

    public static void fetch(String prefix, String suffix) {
	JSONObject container = null;;
	try {
	    URL theURL = new URL(prefix + getOAuthToken() + suffix);
	    BufferedReader reader = new BufferedReader(new InputStreamReader(theURL.openConnection().getInputStream()));

	    container = new JSONObject(new JSONTokener(reader));
	} catch (Exception e) {
	    logger.error("exception:", e);
	}
	logger.info("conversations: " + container.toString(3));
    }
}
