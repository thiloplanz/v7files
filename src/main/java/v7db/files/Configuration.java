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

package v7db.files;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Configuration {

	private static final Properties props = new Properties();

	/**
	 * For every property starting with "-D" (such as "-DDEBUG.MONGO") set it as
	 * a system property (unless already set), and remove it from the properties
	 * object
	 */
	static void setSystemProperties(Properties properties) {
		Iterator<Entry<Object, Object>> it = properties.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Object, Object> e = it.next();
			String key = (String) e.getKey();
			if (key.startsWith("-D")) {
				String kk = key.substring(2);
				if (System.getProperty(kk) == null)
					System.setProperty(kk, (String) e.getValue());
				it.remove();
			}
		}
	}

	static void init(Properties settings) throws IOException {
		props.clear();
		props.load(Main.class.getResourceAsStream("defaults.properties"));
		if (settings != null)
			props.putAll(settings);
		props.putAll(System.getProperties());
		// configure Log4j
		PropertyConfigurator.configure(props);
	}

	static Properties getProperties() {
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

	public static Set<String> checkEndpoints() {
		Set<String> result = new TreeSet<String>();
		List<String> duplicates = new ArrayList<String>();
		for (String ep : getArrayProperty("webdav.endpoints")) {
			if (!result.add(ep)) {
				duplicates.add(ep);
			}
		}
		for (String ep : getArrayProperty("formpost.endpoints")) {
			if (!result.add(ep)) {
				duplicates.add(ep);
			}
		}
		if (duplicates.isEmpty())
			return result;
		throw new IllegalArgumentException("duplicate endpoint definition for "
				+ StringUtils.join(duplicates, ","));

	}

	public static Properties getEndpointProperties(String endpoint) {
		return new EndpointProperties(endpoint, props);
	}

	public static final Mongo getMongo() throws UnknownHostException,
			MongoException {
		Mongo mongo = new Mongo();
		return mongo;
	}

}
