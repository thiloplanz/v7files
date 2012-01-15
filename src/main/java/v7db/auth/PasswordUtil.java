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

package v7db.auth;

import org.apache.commons.lang3.StringUtils;

import arlut.csd.crypto.MD5Crypt;

public class PasswordUtil {

	/**
	 * Checks the password against the password digest (which can be in a number
	 * of supported formats, they will all the checked in turn).
	 * 
	 * @return true, if the password matches the digest
	 */
	public static boolean check(char[] password, Object digest) {
		if (digest instanceof String) {
			String str = (String) digest;
			// UNIX MD5 crypt
			if ((str.startsWith("$1$") || str.startsWith("$apr1$"))
					&& MD5Crypt.verifyPassword(new String(password), str))
				return true;
			if ((str.startsWith("{Crypt}") && check(password, StringUtils
					.substringAfter(str, "{Crypt}"))))
				return true;
		}
		return false;
	}
}
