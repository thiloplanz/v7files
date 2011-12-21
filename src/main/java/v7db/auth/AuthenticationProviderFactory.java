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

package v7db.auth;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class AuthenticationProviderFactory {

	public static AuthenticationProvider getAuthenticationProvider(
			Properties props) {
		String p = props.getProperty("auth.provider");
		if (StringUtils.isBlank(p))
			return null;
		if ("demo".equals(p)) {
			return new DemoAuthenticationProvider(props);
		}
		throw new SecurityException("no such authentication provider " + p);
	}

}
