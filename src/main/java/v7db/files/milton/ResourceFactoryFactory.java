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

import java.util.Properties;

import com.bradmcevoy.http.AuthenticationService;
import com.bradmcevoy.http.ResourceFactory;
import com.bradmcevoy.http.webdav.DefaultWebDavResponseHandler;
import com.bradmcevoy.http.webdav.WebDavResponseHandler;

public class ResourceFactoryFactory implements
		com.bradmcevoy.http.ResourceFactoryFactory {

	public ResourceFactory createResourceFactory() {
		Properties props = MiltonServlet.endpointProperties.get();

		String mode = props.getProperty("v7files.tenants");
		if ("single".equals(mode))
			return new v7db.files.milton.ResourceFactory();

		if ("path".equals(mode))
			return new PathMultiTenantResourceFactory();

		throw new IllegalArgumentException("unsupported tenancy mode: " + mode);

	}

	public WebDavResponseHandler createResponseHandler() {
		return new DefaultWebDavResponseHandler(new AuthenticationService());
	}

	public void init() {
	}

}
