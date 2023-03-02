package com.example.demo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

@Service
public class ConnectToSQLServer {

	@Value("${dbstring}")
	private String dbstring;

	@Value("${user}")
	private String user;

	@Value("${pass}")
	private String pass;

	public int TestConnection() {

		Connection conn = null;
		ResultSet result = null;
		PreparedStatement preparedStatement = null;

		try {
			conn = DriverManager.getConnection(dbstring, user, pass);
			String query = "select sSettingsValue from settings where sSettingsName in(?,?) order by sSettingsName";
			preparedStatement = conn.prepareStatement(query);
			System.out.println("Connected to SQL Server");
			preparedStatement.setString(1, "GLODOMAINNAME");
			preparedStatement.setString(2, "GLODOMAINUSERNAME");
			result = preparedStatement.executeQuery();

		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return 1;
	}
	
	@GetMapping("/gethostnameandip")
	public ResponseEntity<String> getHostname() throws UnknownHostException {
		InetAddress id = InetAddress.getLocalHost();
		String hostname = id.getHostName();
		String ip = id.getHostAddress();

		return new ResponseEntity<String>("hostname - " + hostname + " and ip - " + ip, HttpStatus.OK);
	}
}