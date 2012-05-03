/**
 * Copyright (c) 2012, Thilo Planz. All rights reserved.
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
import java.util.Properties;

import jmockmongo.MockMongo;

public class UnitTestSupport {

	private static final Properties defaultProps = new Properties();
	static {
		try {
			defaultProps.load(Main.class
					.getResourceAsStream("defaults.properties"));
			defaultProps.put("db.uri", MockMongo.DEFAULT_URI.toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Properties getDefaultProperties() {
		return (Properties) defaultProps.clone();
	}

	public static void initConfiguration(Properties props) {
		try {
			Configuration.init(props);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
