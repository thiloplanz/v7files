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

package v7db.files.mongodb;

import java.util.Properties;

import junit.framework.TestCase;
import v7db.auth.AuthenticationToken;
import v7db.files.AuthorisationProvider;
import v7db.files.AuthorisationProviderFactory;
import v7db.files.mongodb.V7File;

import com.mongodb.BasicDBObject;

public abstract class AuthorisationProviderTest extends TestCase {

	final String aclProvider;

	public AuthorisationProviderTest(String aclProvider) {
		this.aclProvider = aclProvider;
	}

	public void testAnonymousCannotDoAnythingByDefault() {
		Properties props = new Properties();
		props.setProperty("acl.provider", aclProvider);
		AuthorisationProvider auth = AuthorisationProviderFactory
				.getAuthorisationProvider(props);

		V7File dummyFile = new V7File(null, new BasicDBObject(), null);

		assertFalse(auth.authoriseOpen(dummyFile, null));
		assertFalse(auth
				.authoriseOpen(dummyFile, AuthenticationToken.ANONYMOUS));
		assertFalse(auth.authoriseRead(dummyFile, null));
		assertFalse(auth
				.authoriseRead(dummyFile, AuthenticationToken.ANONYMOUS));
		assertFalse(auth.authoriseWrite(dummyFile, null));
		assertFalse(auth.authoriseWrite(dummyFile,
				AuthenticationToken.ANONYMOUS));

	}

}
