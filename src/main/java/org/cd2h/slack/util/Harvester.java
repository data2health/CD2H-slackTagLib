package org.cd2h.slack.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.uiowa.extraction.LocalProperties;
import edu.uiowa.extraction.PropertyLoader;

public class Harvester extends SlackAPI {
	static Logger logger = LogManager.getLogger(Harvester.class);
    protected static LocalProperties prop_file = null;
    static Connection conn = null;

    static String prefix = "https://slack.com/api/";
    static String midfix = "?token=";
    static String suffix = "&pretty=1";
    
	static public void main(String[] args) throws ClassNotFoundException, SQLException {
		prop_file = PropertyLoader.loadProperties("slack");
		conn = getConnection();

		switch (args[1]) {
		case "channels":
			fetchChannels();
			break;
		case "users":
			fetchUsers();
			break;
		case "members":
			fetchMembers();
			break;
		case "messages":
			fetchMessages();
			break;
		default:
			break;
		}
	}
    
	public static void fetchChannels() throws SQLException {
		simpleStmt("truncate cd2h_slack.channel_raw");
		JSONObject result = fetch("conversations.list", "");
		for (int i = 0; i < result.getJSONArray("channels").length(); i++) {
			JSONObject channel = result.getJSONArray("channels").getJSONObject(i);
			logger.info("channel: " + channel.toString(3));

			PreparedStatement stmt = conn.prepareStatement("insert into cd2h_slack.channel_raw values(?::jsonb)");
			stmt.setString(1, channel.toString(3));
			stmt.execute();
			stmt.close();
		}
		simpleStmt("refresh materialized view cd2h_slack.channel");

	}

	public static void fetchUsers() throws SQLException {
		simpleStmt("truncate cd2h_slack.user_raw");
		String cursor = null;
		do {
			JSONObject result = fetch("users.list", "&limit=1000" + (cursor == null ? "" : "&cursor=" + cursor));
			for (int i = 0; i < result.getJSONArray("members").length(); i++) {
				JSONObject user = result.getJSONArray("members").getJSONObject(i);
				logger.info("user: " + user.toString(3));

				PreparedStatement stmt = conn.prepareStatement("insert into cd2h_slack.user_raw values(?::jsonb)");
				stmt.setString(1, user.toString(3));
				stmt.execute();
				stmt.close();
			}
			JSONObject response = result.optJSONObject("response_metadata");
			cursor = response == null ? null : response.optString("next_cursor");
			logger.info("cursor: " + cursor);
		} while (cursor != null && cursor.length() > 0);
		simpleStmt("refresh materialized view cd2h_slack.person");
	}

	public static void fetchMembers() throws SQLException {
		simpleStmt("truncate cd2h_slack.member");
		PreparedStatement fetchStmt = conn.prepareStatement("select id from cd2h_slack.channel where not archived");
		ResultSet fetchRS = fetchStmt.executeQuery();
		while (fetchRS.next()) {
			String channelID = fetchRS.getString(1);
			try {
				JSONObject result = fetch("conversations.members", "&channel=" + channelID + "&limit=1000");
				for (int i = 0; i < result.getJSONArray("members").length(); i++) {
					String user = result.getJSONArray("members").getString(i);
					logger.info("\tuser: " + user);

					PreparedStatement stmt = conn.prepareStatement("insert into cd2h_slack.member values(?,?)");
					stmt.setString(1, channelID);
					stmt.setString(2, user);
					stmt.execute();
					stmt.close();
				}
			} catch (Exception e) {
				logger.error("error raised fetching members:", e);
			}
		}
		fetchStmt.close();
	}

    public static void fetchMessages() throws SQLException {
    	PreparedStatement channelStmt = conn.prepareStatement("select channel.name from cd2h_slack.person,cd2h_slack.member,cd2h_slack.channel where person.id=member.user_id and member.channel_id=channel.id and display_name='analytics' order by name");
    	ResultSet channelRS = channelStmt.executeQuery();
    	while (channelRS.next()) {
    		String channel = channelRS.getString(1);

    		PreparedStatement fetchStmt = conn.prepareStatement("select id from cd2h_slack.channel where name=?");
    		fetchStmt.setString(1, channel);
    		ResultSet fetchRS = fetchStmt.executeQuery();
    		while (fetchRS.next()) {
    			String channelID = fetchRS.getString(1);
    			String cursor = null;
    			do {
    				JSONObject result = fetch("conversations.history", "&channel=" + channelID + "&limit=1000"+(cursor == null ? "" : "&cursor="+cursor));
    				for (int i = 0; i < result.getJSONArray("messages").length(); i++) {
    					JSONObject message = result.getJSONArray("messages").getJSONObject(i);
    					logger.info("message: " + message.toString(3));

    					PreparedStatement stmt = conn.prepareStatement("insert into cd2h_slack.message_raw values(?,?::jsonb)");
    					stmt.setString(1, channelID);
    					stmt.setString(2, message.toString(3));
    					stmt.execute();
    					stmt.close();
    				}
    				JSONObject response = result.optJSONObject("response_metadata");
    				cursor = response == null ? null : response.optString("next_cursor");
    			} while (cursor != null);
    		}
    		fetchStmt.close();
    	}
    	channelStmt.close();
    }

	public static JSONObject fetch(String entity, String params) {
		JSONObject container = null;
		try {
			URL theURL = new URL(prefix + entity + midfix + getOAuthToken() + suffix + params);
			BufferedReader reader = new BufferedReader(new InputStreamReader(theURL.openConnection().getInputStream()));

			container = new JSONObject(new JSONTokener(reader));
		} catch (Exception e) {
			logger.error("exception:", e);
		}
		logger.info("container: " + container.toString(3));
		return container;
	}

	public static void simpleStmt(String queryString) {
		try {
			logger.info("executing " + queryString + "...");
			PreparedStatement beginStmt = conn.prepareStatement(queryString);
			beginStmt.executeUpdate();
			beginStmt.close();
		} catch (Exception e) {
			logger.error("Error in database initialization: " + e);
			e.printStackTrace();
		}
	}

	public static Connection getConnection() throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		Properties props = new Properties();
		props.setProperty("user", prop_file.getProperty("jdbc.user"));
		props.setProperty("password", prop_file.getProperty("jdbc.password"));
		Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
		return conn;
	}
}
