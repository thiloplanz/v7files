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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Configuration {

	public static final Properties getProperties() throws IOException {
		Properties params = new Properties();
		params.load(Main.class.getResourceAsStream("defaults.properties"));
		params.putAll(System.getProperties());

		return params;
	}

	public static final Mongo getMongo() throws UnknownHostException,
			MongoException {
		Mongo mongo = new Mongo();
		return mongo;
	}

}
