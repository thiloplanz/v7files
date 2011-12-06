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
import v7db.files.Configuration;
import v7db.files.V7File;
import v7db.files.V7GridFS;

import com.bradmcevoy.http.ApplicationConfig;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.Initable;
import com.bradmcevoy.http.MiltonServlet;
import com.bradmcevoy.http.Resource;
import com.mongodb.Mongo;

class ResourceFactory implements com.bradmcevoy.http.ResourceFactory, Initable {

	private Mongo mongo;

	private V7GridFS fs;

	private String ROOT;

	private String endpoint;

	public void init(ApplicationConfig config, HttpManager manager) {
		try {
			mongo = Configuration.getMongo();
			fs = new V7GridFS(mongo.getDB("test"));
			endpoint = config.getInitParameter("v7files.endpoint");

			ROOT = Configuration.getProperties().getProperty(
					"v7files.endpoint." + endpoint + ".root", endpoint);
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
			return new FileResource(f);

		return new FolderResource(f);
	}

	public void destroy(HttpManager manager) {
		if (mongo != null)
			mongo.close();

	}

}
