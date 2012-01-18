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

import static org.apache.commons.lang3.StringUtils.substringAfter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import v7db.files.Configuration;
import v7db.files.V7GridFS;

import com.bradmcevoy.http.ApplicationConfig;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.Initable;
import com.bradmcevoy.http.MiltonServlet;
import com.bradmcevoy.http.Resource;
import com.mongodb.Mongo;

/**
 * A ResourceFactory that manages separate ResourceFactory instances for every
 * tenant, and dispatches accordingly
 * 
 */
public class PathMultiTenantResourceFactory implements
		com.bradmcevoy.http.ResourceFactory, Initable {

	private Mongo mongo;

	private final Map<String, ResourceFactory> paths = new ConcurrentHashMap<String, ResourceFactory>();

	private ApplicationConfig config;

	private static final Logger log = LoggerFactory
			.getLogger(PathMultiTenantResourceFactory.class);

	public Resource getResource(String host, String p) {
		String servletPath = MiltonServlet.request().getServletPath();
		String[] path = substringAfter(p, servletPath).split("/");
		if (path.length < 2) {
			// must have at least [ ROOT, tenant ]
			return null;
		}
		String tenant = path[1];
		MDC.put("tenant", tenant);
		String realPath = "/"
				+ substringAfter(p, servletPath + "/" + tenant + "/");

		ResourceFactory t = paths.get(tenant);
		if (t != null)
			return t.getResource(host, realPath);

		// check if the tenant exists
		V7GridFS fs = V7GridFS.getIfExists(mongo, tenant);
		if (fs == null) {
			log.warn("tried to access non-existing tenant " + path[1] + " for "
					+ realPath);
			return null;
		}

		t = new ResourceFactory();
		t.init(config, null);
		paths.put(tenant, t);
		return t.getResource(host, realPath);
	}

	public void destroy(HttpManager manager) {
		if (mongo != null)
			mongo.close();
	}

	public void init(ApplicationConfig config, HttpManager manager) {
		this.config = config;
		try {
			this.mongo = Configuration.getMongo();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
