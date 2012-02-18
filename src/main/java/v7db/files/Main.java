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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class Main {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {

		// find a [-f config.properties]
		File configFile = null;
		{
			int flag = ArrayUtils.indexOf(args, "-f");
			if (flag > -1) {
				configFile = new File(args[flag + 1]);
				args = ArrayUtils.remove(ArrayUtils.remove(args, flag + 1),
						flag);
			}
		}

		if (args.length == 0) {
			System.err.println("v7files [-f config.properties] <command>");
			System.exit(1);
		}

		String command = args[0];

		Class<?> commandClass;
		try {
			commandClass = Class.forName("v7db.files."
					+ StringUtils.capitalize(command) + "Command");
		} catch (ClassNotFoundException e) {
			System.err.println("unsupported command '" + command + "'");
			System.exit(1);
			return;
		}

		if (configFile != null) {
			Properties config = new Properties();
			config.load(new FileInputStream(configFile));
			Configuration.init(config);
		} else {
			Configuration.init(null);
		}

		commandClass.getMethod("main", String[].class).invoke(null,
				(Object) args);

	}
}
