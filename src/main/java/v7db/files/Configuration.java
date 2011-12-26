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

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Configuration {

	private static final Properties props = new Properties();

	static void init(Properties settings) throws IOException {
		props.clear();
		props.load(Main.class.getResourceAsStream("defaults.properties"));
		if (settings != null)
			props.putAll(settings);
		props.putAll(System.getProperties());
		// configure Log4j
		PropertyConfigurator.configure(props);
	}

	public static Properties getProperties() {
		return props;
	}

	public static String getProperty(String key) {
		return props.getProperty(key);
	}

	/**
	 * property string value is split by comma and trimmed for space
	 */
	public static String[] getArrayProperty(String key) {
		return StringUtils.stripAll(StringUtils.split(getProperty(key), ','));
	}

	public static String getEndpointProperty(String endpoint, String key) {
		String p = getProperty(endpoint + "." + key);
		if (p == null)
			return getProperty(key);
		return p;
	}

	public static final Mongo getMongo(String endpoint)
			throws UnknownHostException, MongoException {
		Mongo mongo = new Mongo();
		return mongo;
	}

}
