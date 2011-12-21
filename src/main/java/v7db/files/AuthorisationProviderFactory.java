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

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class AuthorisationProviderFactory {

	public static AuthorisationProvider getAuthorisationProvider(
			Properties props, String endpoint) {
		String p = props.getProperty("acl.provider");
		if (StringUtils.isBlank(p))
			throw new SecurityException("no authorisation provider defined");
		if ("acl".equals(p))
			return new AclAuthorisationProvider(props, endpoint);
		if ("global".equals(p))
			return new GlobalAuthorisationProvider(props, endpoint);
		if ("trusted".equals(p))
			return new TrustedAuthorisationProvider();

		throw new SecurityException("no such authorisation provider " + p);
	}

}
