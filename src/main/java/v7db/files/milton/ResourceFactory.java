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

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;

import org.slf4j.MDC;

import v7db.auth.AuthenticationProvider;
import v7db.auth.AuthenticationProviderFactory;
import v7db.auth.AuthenticationToken;
import v7db.files.AuthorisationProvider;
import v7db.files.AuthorisationProviderFactory;
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

	private String endpointName;

	private String dbName;

	private AuthenticationProvider authentication;

	private AuthorisationProvider authorisation;

	private boolean fakeLocking = false;

	public void init(ApplicationConfig config, HttpManager manager) {
		try {
			endpoint = config.getInitParameter("v7files.endpoint");

			endpointName = defaultIfBlank(substringAfterLast(endpoint, "/"),
					"/");

			mongo = Configuration.getMongo(endpoint);
			dbName = getProperty("mongo.db");
			fs = new V7GridFS(mongo.getDB(dbName));

			ROOT = getProperty("root");
			if (ROOT == null)
				ROOT = endpoint;

			authentication = AuthenticationProviderFactory
					.getAuthenticationProvider(Configuration.getProperties());

			authorisation = AuthorisationProviderFactory
					.getAuthorisationProvider(Configuration.getProperties(),
							endpoint);

			fakeLocking = "fake".equals(getProperty("locking.provider"));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public Resource getResource(String host, String path) {

		path = substringAfter(path, MiltonServlet.request().getServletPath());

		if ("/".equals(path)) {
			return fakeLocking ? new LockableFolderResource(endpointName, fs
					.getFile(ROOT), this) : new FolderResource(endpointName, fs
					.getFile(ROOT), this);
		}

		String[] p = path.split("/");
		p[0] = ROOT;

		V7File f = fs.getFile(p);
		if (f == null)
			return null;

		if (f.hasContent())
			return fakeLocking ? new LockableFileResource(f, this)
					: new FileResource(f, this);

		return fakeLocking ? new LockableFolderResource(f, this)
				: new FolderResource(f, this);
	}

	public void destroy(HttpManager manager) {
		if (mongo != null)
			mongo.close();

	}

	String getProperty(String name) {
		return Configuration.getEndpointProperty(endpoint, name);
	}

	String getRealm() {
		return getProperty("auth.realm");
	}

	boolean authorise(V7File file, Request request, Method method, Auth auth) {

		AuthenticationToken tag = auth == null ? null
				: (AuthenticationToken) auth.getTag();
		switch (method) {
		case GET:
		case PROPFIND:
			return authorisation.authoriseRead(file, tag);
		case POST:
		case PUT:
		case MKCOL:
		case MOVE:
		case DELETE:
		case LOCK:
			return authorisation.authoriseWrite(file, tag);
		case UNLOCK:
			// unlock only works if you have the lock token, so no extra
			// authorisation required
			return true;
		default:
			System.err.println("acl not implemented for " + method);
			return false;
		}

	}

	AuthenticationToken authenticate(String user, String password) {
		// Cyberduck does BasicAuth with "anonymous"
		// not sure if that is good, but here we go ...
		// we cannot return null because that would "fail" the anonymous login
		if ("anonymous".equals(user))
			return AuthenticationToken.ANONYMOUS;

		if (authentication == null)
			return null;

		AuthenticationToken auth = authentication.authenticate(user, password);

		if (auth != null)
			MDC.put("user", auth.getUsername());

		return auth;

	}

}
