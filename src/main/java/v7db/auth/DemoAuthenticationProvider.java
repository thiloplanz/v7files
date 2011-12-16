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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.stripAll;

import java.util.Properties;

class DemoAuthenticationProvider implements AuthenticationProvider {

	private final Properties props;

	DemoAuthenticationProvider(Properties props) {
		this.props = props;
	}

	public AuthenticationToken authenticate(String username, String password) {
		if (isBlank(username) || isBlank(password))
			return null;
		String checkPassword = props.getProperty("auth.demo.user." + username
				+ ".password");
		if (!password.equals(checkPassword))
			return null;

		return new AuthenticationToken(username,
				(Object[]) stripAll(split(props.getProperty("auth.demo.user."
						+ username + ".roles"), ',')));
	}

}
