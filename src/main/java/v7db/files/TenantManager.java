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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.mongodb.Mongo;

public class TenantManager {

	private final static Logger log = LoggerFactory
			.getLogger(TenantManager.class);

	private final Mongo mongo;

	private final Tenant single;

	private final Map<String, Tenant> paths;

	public TenantManager(Mongo mongo, Properties props) {
		this.mongo = mongo;
		String mode = props.getProperty("v7files.tenants");
		if ("single".equals(mode)) {
			single = new Tenant(new V7GridFS(mongo.getDB(props
					.getProperty("mongo.db"))));
			paths = null;
		} else if ("path".equals(mode)) {
			single = null;
			paths = new ConcurrentHashMap<String, Tenant>();
		} else {
			throw new IllegalArgumentException("unsupported tenancy mode: "
					+ mode);
		}
	}

	public V7File getFile(String... path) {
		if (single != null)
			return single.getFile(path);
		if (path.length < 2) {
			// must have at least [ ROOT, tenant ]
			return null;
		}
		MDC.put("tenant", path[1]);
		Tenant pathTenant = paths.get(path[1]);
		String[] realPath = ArrayUtils.remove(path, 1);
		if (pathTenant == null) {
			V7GridFS fs = V7GridFS.getIfExists(mongo, path[1]);
			if (fs == null) {
				log.warn("tried to access non-existing tenant " + path[1]
						+ " for " + StringUtils.join(realPath, '/'));
				return null;
			}
			pathTenant = new Tenant(fs);
			paths.put(path[1], pathTenant);
		}

		return pathTenant.getFile(realPath);

	}

	static class Tenant {

		private final V7GridFS fs;

		Tenant(V7GridFS fs) {
			this.fs = fs;
		}

		V7File getFile(String... path) {
			return fs.getFile(path);
		}

	}

}
