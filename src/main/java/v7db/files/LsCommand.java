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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import v7db.files.mongodb.V7File;
import v7db.files.mongodb.V7GridFS;

import com.mongodb.MongoException;

class LsCommand {

	public static void main(String[] args) throws MongoException, IOException {

		if (args.length != 3) {
			System.err.println("List a file or directory:");
			System.err.println("  ls <root> <path>");
			System.exit(1);
		}

		V7GridFS fs = new V7GridFS(Configuration.getMongo().getDB(
				Configuration.getProperty("mongo.db")));

		String root = args[1];
		String path = args[2];
		String[] fullPath = ArrayUtils.add(StringUtils.split(path, '/'), 0,
				root);
		V7File file = fs.getFile(fullPath);
		if (file == null) {
			System.err.println("file not found");
			System.exit(1);
		}
		if (file.hasContent()) {
			System.out.format("    %10d %80s\n", file.getLength(), file
					.getName());
		}
		List<V7File> children = file.getChildren();
		Collections.sort(children, new Comparator<V7File>() {

			public int compare(V7File o1, V7File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		for (V7File child : children) {
			if (child.getLength() == null)
				System.out.format("d      %10s %80s\n", "", child.getName());
		}
		for (V7File child : children) {
			if (child.getLength() != null)
				System.out.format("-      %10d %80s %10s\n", child.getLength(),
						child.getName(), child.getDigest().substring(0, 10));
		}

	}

}
