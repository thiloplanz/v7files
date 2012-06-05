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

import v7db.files.mongodb.V7File;
import v7db.files.mongodb.V7GridFS;

import com.mongodb.DB;

class MkdirCommand {

	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.err.println("Create a directory/folder");
			System.err.println("  mkdir <root> <path>");
			System.exit(1);
		}

		DB db = Configuration.getMongo().getDB(
				Configuration.getProperty("mongo.db"));

		V7GridFS fs = new V7GridFS(db);

		String[] path = CopyCommand.getPath(args[1], args[2]);

		V7File existing = fs.getFile(path);

		if (existing != null) {
			if (existing.hasContent()) {
				throw new IOException(args[2]
						+ " already exists (and is a file)");
			}
			throw new IOException("directory " + args[2] + " already exists");
		}

		V7File parent = CopyCommand.getParent(fs, path);
		fs.addFolder(parent.getId(), path[path.length - 1]);

	}

}
