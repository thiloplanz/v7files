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

package v7db.files.milton;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import v7db.files.Configuration;

import com.bradmcevoy.http.AuthenticationService;
import com.bradmcevoy.http.Handler;
import com.bradmcevoy.http.HttpExtension;
import com.bradmcevoy.http.http11.Http11Protocol;
import com.bradmcevoy.http.webdav.DefaultWebDavResponseHandler;

public class MiltonServlet extends com.bradmcevoy.http.MiltonServlet {

	private static Logger log = LoggerFactory.getLogger(MiltonServlet.class);

	// our resourceFactoryFactory needs the endpoint configuration
	final static ThreadLocal<Properties> endpointProperties = new ThreadLocal<Properties>();

	private String dbName;

	@Override
	public void service(ServletRequest servletRequest,
			ServletResponse servletResponse) throws ServletException,
			IOException {
		long time = System.nanoTime();
		HttpServletRequest r = (HttpServletRequest) servletRequest;
		HttpServletResponse s = (HttpServletResponse) servletResponse;
		try {
			MDC.put("tenant", dbName);
			MDC.put("path", r.getRequestURI());
			MDC.put("method", r.getMethod());
			super.service(servletRequest, servletResponse);
		} finally {
			long taken = System.nanoTime() - time;
			try {
				log.info("request took " + taken / (1000 * 1000)
						+ " ms, status " + s.getStatus());
			} catch (Throwable e) {
				// httpunit does not implement getStatus() (@since Servlet API
				// 3.0 )
				log.info("request took " + taken / (1000 * 1000) + " ms");
			}
			MDC.clear();
		}
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		String endpoint = config.getInitParameter("webdav.endpoint");
		Properties ep = Configuration.getEndpointProperties(endpoint);
		dbName = ep.getProperty("mongo.db");
		try {
			endpointProperties.set(ep);
			super.init(config);
		} finally {
			endpointProperties.remove();
		}
		// http://stackoverflow.com/questions/8380324/
		httpManager.getHandlers().setEnableExpectContinue(false);

		// http://jira.ettrema.com:8080/browse/MIL-11

		handlers: for (HttpExtension x : httpManager.getHandlers()) {
			if (x instanceof Http11Protocol) {
				Http11Protocol p = (Http11Protocol) x;
				for (Handler h : x.getHandlers()) {
					if (h instanceof com.bradmcevoy.http.http11.GetHandler) {
						httpManager.addFilter(0, new GetHandler(
								new DefaultWebDavResponseHandler(
										new AuthenticationService()), p
										.getHandlerHelper()));
						break handlers;
					}
				}
			}
		}
	}

}
