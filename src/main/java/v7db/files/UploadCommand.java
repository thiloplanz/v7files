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
import java.io.IOException;

import org.apache.commons.codec.binary.Hex;

import v7db.files.aws.GridFSContentStorageWithS3;

import com.mongodb.MongoException;

class UploadCommand {

	public static void main(String[] args) throws MongoException, IOException {

		if (args.length < 2) {
			System.err
					.println("Upload the contents of one or more files and print their SHA digests:");
			System.err
					.println("  by hash:   upload <file> [file] [file] [...]");
			System.exit(1);
		}

		GridFSContentStorageWithS3 storage = GridFSContentStorageWithS3
				.configure(Configuration.getMongo(), Configuration
						.getProperties());

		for (int i = 1; i < args.length; i++) {
			File f = new File(args[i]);
			if (f.isFile() && f.canRead()) {
				try {
					String sha = Hex.encodeHexString(storage.storeInS3(f, f
							.getName(), null, null));
					System.out.format("-      %10d %80s %40s\n", f.length(), f
							.getName(), sha);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
