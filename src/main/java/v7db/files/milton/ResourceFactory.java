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

package v7db.files.milton;

import static org.apache.commons.lang3.StringUtils.substringAfter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import v7db.auth.AuthenticationProvider;
import v7db.auth.AuthenticationProviderFactory;
import v7db.auth.AuthenticationToken;
import v7db.files.Configuration;
import v7db.files.V7File;
import v7db.files.V7GridFS;

import com.bradmcevoy.http.ApplicationConfig;
import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.Initable;
import com.bradmcevoy.http.MiltonServlet;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request.Method;
import com.mongodb.Mongo;

class ResourceFactory implements com.bradmcevoy.http.ResourceFactory, Initable {

	private Mongo mongo;

	private V7GridFS fs;

	private String ROOT;

	private String endpoint;

	private AuthenticationProvider authentication;

	public void init(ApplicationConfig config, HttpManager manager) {
		try {
			endpoint = config.getInitParameter("v7files.endpoint");

			mongo = Configuration.getMongo(endpoint);
			fs = new V7GridFS(mongo.getDB(getProperty("mongo.db")));

			ROOT = getProperty("root");
			if (ROOT == null)
				ROOT = endpoint;

			authentication = AuthenticationProviderFactory
					.getProvider(Configuration.getProperties());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public Resource getResource(String host, String path) {

		path = substringAfter(path, MiltonServlet.servletConfig()
				.getServletContext().getContextPath());
		String[] p;

		if ("/".equals(path)) {
			p = new String[] { ROOT };
		} else {
			p = path.split("/");
			p[0] = ROOT;
		}
		V7File f = fs.getFile(p);
		if (f == null)
			return null;

		if (f.hasContent())
			return new FileResource(f, this);

		return new FolderResource(f, this);
	}

	public void destroy(HttpManager manager) {
		if (mongo != null)
			mongo.close();

	}

	String getProperty(String name) {
		return Configuration.getEndpointProperty(endpoint, name);
	}

	private String getAnonymousUser() {
		return getProperty("auth.anonymous");
	}

	String getRealm() {
		return getProperty("auth.realm");
	}

	boolean authorise(Request request, Method method, Auth auth) {
		AuthenticationToken tag = auth == null ? null
				: (AuthenticationToken) auth.getTag();
		Object[] roles;
		if (tag == null) {
			String user = getAnonymousUser();
			if (StringUtils.isBlank(user))
				return false;
			roles = new String[] { user };
		} else {
			roles = tag.getRoles();
		}
		if (method != Request.Method.GET) {
			return false;
		}
		// for now, just endpoint global settings, no per-file settings yet
		String[] acl = StringUtils.stripAll(StringUtils.split(
				getProperty("acl.read"), ','));
		for (Object role : roles) {
			if (ArrayUtils.contains(acl, role))
				return true;
		}
		return false;
	}

	AuthenticationToken authenticate(String user, String password) {
		if (authentication == null)
			return null;

		return authentication.authenticate(user, password);

	}

}
