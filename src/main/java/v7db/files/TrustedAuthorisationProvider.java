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

import v7db.auth.AuthenticationToken;

class TrustedAuthorisationProvider extends GlobalAuthorisationProvider {

	TrustedAuthorisationProvider(Properties props) {
		super(props);
	}

	/**
	 * @return true unless the user is anonymous, and anonymous access is not
	 *         allowed
	 */
	private boolean checkAnonymous(AuthenticationToken user) {
		if (user != null && user.getUsername() != null)
			return true;
		return getRoles(user) != null;
	}

	@Override
	public boolean authoriseOpen(V7File resource, AuthenticationToken user) {
		return checkAnonymous(user);
	}

	@Override
	public boolean authoriseRead(V7File resource, AuthenticationToken user) {
		return checkAnonymous(user);
	}

	@Override
	public boolean authoriseWrite(V7File resource, AuthenticationToken user) {
		return checkAnonymous(user);
	}

}
