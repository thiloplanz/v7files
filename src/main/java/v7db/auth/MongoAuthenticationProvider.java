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

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * An AuthenticationProvider that queries a MongoDB database for authentication
 * information (in a configurable way)
 * 
 */

public class MongoAuthenticationProvider implements AuthenticationProvider {

	private final DBCollection collection;

	private final String username_field;

	private final String password_field;

	private String getRequiredProperty(Properties props, String name) {
		String v = props.getProperty(name);
		if (StringUtils.isBlank(v))
			throw new IllegalArgumentException(
					"MongoAuthenticationProvider: missing required parameter "
							+ name);
		return v;
	}

	public MongoAuthenticationProvider(Mongo mongo, Properties props) {
		String dbName = props.getProperty("mongo.db");
		collection = mongo.getDB(dbName).getCollection(
				getRequiredProperty(props, "auth.mongo.collection"));
		username_field = getRequiredProperty(props, "auth.mongo.username");
		password_field = getRequiredProperty(props, "auth.mongo.password");

	}

	public AuthenticationToken authenticate(String username, String password) {
		DBObject found = collection.findOne(new BasicDBObject(username_field,
				username), new BasicDBObject(password_field, true));
		if (found == null)
			return null;
		if (!PasswordUtil.check(password.toCharArray(), found
				.get(password_field)))
			return null;

		return new AuthenticationToken(username);
	}
}
