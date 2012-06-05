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

import org.apache.commons.lang3.ArrayUtils;

import v7db.auth.AuthenticationToken;
import v7db.files.mongodb.V7File;

class AclAuthorisationProvider implements AuthorisationProvider {

	private final GlobalAuthorisationProvider global;

	public AclAuthorisationProvider(Properties props) {
		global = new GlobalAuthorisationProvider(props);
	}

	private Boolean authorise(V7File resource, AuthenticationToken user,
			String permission) {
		Object[] roles = global.getRoles(user);
		if (roles == null)
			return false;

		Object[] acl = resource.getEffectiveAcl(permission);
		if (acl == null)
			return null;
		for (Object role : roles) {
			if (ArrayUtils.contains(acl, role))
				return true;
		}
		return false;
	}

	public boolean authoriseOpen(V7File resource, AuthenticationToken user) {
		Object[] acl = resource.getAcl("open");
		if (acl == null) {
			// no ACL set at all,
			// inherit from parent
			V7File parent = resource.getParent();
			if (parent != null)
				return authoriseOpen(parent, user);
			return global.authoriseOpen(resource, user);
		}
		if (acl.length == 0) {
			// "open" not set, default to "read" (which must be set)
			return authoriseRead(resource, user);
		}

		Boolean result = authorise(resource, user, "open");
		if (result == null)
			return global.authoriseRead(resource, user);
		return result;
	}

	public boolean authoriseRead(V7File resource, AuthenticationToken user) {
		V7File parent = resource.getParent();
		if (parent != null) {
			if (!authoriseOpen(parent, user))
				return false;
		}
		Boolean result = authorise(resource, user, "read");
		if (result == null)
			return global.authoriseRead(resource, user);
		return result;
	}

	public boolean authoriseWrite(V7File resource, AuthenticationToken user) {
		V7File parent = resource.getParent();
		if (parent != null) {
			if (!authoriseOpen(parent, user))
				return false;
		}
		Boolean result = authorise(resource, user, "write");
		if (result == null)
			return global.authoriseRead(resource, user);
		return result;
	}

}
