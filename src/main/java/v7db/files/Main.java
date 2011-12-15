/**
 * Copyright (c) 2011, Thilo Planz. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package v7db.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.servlet.ServletHandler;

import v7db.files.milton.MiltonServlet;

public class Main {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length == 2 && "-f".equals(args[0])) {
			File configFile = new File(args[1]);
			Properties config = new Properties();
			config.load(new FileInputStream(configFile));
			Configuration.init(config);
		} else if (args.length == 0) {
			Configuration.init(null);
		} else {
			System.err.println("v7files [-f config.properties]");
			System.exit(1);
		}

		ServletHandler handler = new ServletHandler();
		handler.setServletInstance(new MiltonServlet());

		String endpoint = Configuration.getProperty("v7files.endpoints");
		if ("/".equals(endpoint)) {
			handler.setContextPath("");
		} else {
			handler.setContextPath(endpoint);
		}
		handler.addInitParameter("v7files.endpoint", endpoint);
		handler.addInitParameter("resource.factory.factory.class",
				Configuration.getProperty("resource.factory.factory.class"));

		final HttpServer server = new HttpServer();
		int port = Integer.parseInt(Configuration.getProperty("http.port"));
		String host = NetworkListener.DEFAULT_NETWORK_HOST;
		final NetworkListener listener = new NetworkListener("grizzly", host,
				port);
		server.addListener(listener);

		final ServerConfiguration config = server.getServerConfiguration();
		config.addHttpHandler(handler, "/");

		server.start();

		try {
			System.in.read();
			server.stop();
		} catch (IOException e) {
			System.err
					.println("STDIN unavailable, continue running in daemon mode");
			try {
				synchronized (args) {
					args.wait(); // forever
				}
			} catch (InterruptedException e1) {
			}
		}
	}
}
