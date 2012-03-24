/**
 * Copyright (c) 2011-2012, Thilo Planz. All rights reserved.
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

import java.io.IOException;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import v7db.files.buckets.BucketsServlet;
import v7db.files.milton.MiltonServlet;

class ServeCommand {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Configuration.checkEndpoints();

		ServletContextHandler handler = new ServletContextHandler();
		handler.setContextPath("/");

		// WebDAV
		{
			String[] endpoints = Configuration
					.getArrayProperty("webdav.endpoints");

			for (String endpoint : endpoints) {

				ServletHolder servlet = new ServletHolder(new MiltonServlet());
				servlet.setInitParameter("webdav.endpoint", endpoint);
				servlet.setInitParameter("resource.factory.factory.class",
						Configuration
								.getProperty("resource.factory.factory.class"));

				handler.addServlet(servlet, endpoint.equals("/") ? "/*"
						: (endpoint + "/*"));
			}

		}

		// FormPost

		{
			String[] endpoints = Configuration
					.getArrayProperty("buckets.endpoints");

			for (String endpoint : endpoints) {

				ServletHolder servlet = new ServletHolder(new BucketsServlet(
						Configuration.getEndpointProperties(endpoint)));
				handler.addServlet(servlet, endpoint.equals("/") ? "/*"
						: (endpoint + "/*"));
			}

		}

		int port = Integer.parseInt(Configuration.getProperty("http.port"));
		final Server server = new Server(port);
		server.setHandler(handler);

		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("failed to start HTTP server");
			try {
				server.stop();
			} catch (Exception f) {
				f.printStackTrace();
				System.exit(1);
			}
		}
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
