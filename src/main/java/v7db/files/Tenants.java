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

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.Mongo;

public class Tenants {

	/**
	 * 
	 * @return null, if the mapped database does not yet exist
	 */
	public static String getTenantDbName(Mongo mongo, Properties props,
			String tenantName) {
		if (props != null && "single".equals(getTenancyMode(props))) {
			return props.getProperty("mongo.db");
		}

		// TODO: some kind of mapping
		final String tenantDbName = tenantName;
		if (StringUtils.isBlank(tenantDbName))
			return null;
		if (mongo.getDB(tenantDbName).collectionExists("v7files"))
			return tenantDbName;
		return null;
	}

	public static String getTenancyMode(Properties props) {
		String mode = props.getProperty("v7files.tenants");
		if ("single".equals(mode))
			return mode;

		if ("path".equals(mode))
			return mode;

		throw new IllegalArgumentException("unsupported tenancy mode: " + mode);
	}

}
