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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.Content;

import com.mongodb.MongoException;

class CatCommand {

	static byte[] decodeSHAPrefix(String shaPrefix) throws DecoderException,
			IOException {
		byte[] id;
		if (shaPrefix.startsWith("BinData(")) {
			id = Base64.decodeBase64(StringUtils.substringBetween(shaPrefix,
					",", ")"));
		} else {
			id = Hex.decodeHex(shaPrefix.toCharArray());
		}
		if (id.length > 20)
			throw new DecoderException("too long");
		return id;
	}

	private static Content findContentByPrefix(MongoContentStorage storage,
			String shaPrefix) throws DecoderException, IOException {
		return storage.findContentByPrefix(decodeSHAPrefix(shaPrefix));
	}

	public static void main(String[] args) throws MongoException, IOException {

		if (args.length != 3) {
			System.err.println("Output the contents of a file:");
			System.err.println("  by name:   cat <root> <path>");
			System.err.println("  by hash:   cat -sha <shaHex>");
			System.exit(1);
		}

		if ("-sha".equals(args[1])) {
			MongoContentStorage storage = new MongoContentStorage(Configuration
					.getMongo().getDB(Configuration.getProperty("mongo.db")));
			String sha = args[2];
			try {
				Content file = findContentByPrefix(storage, sha);
				if (file == null) {
					System.err.println("file not found");
					System.exit(1);
				}
				IOUtils.copy(file.getInputStream(), System.out);
			} catch (DecoderException e) {
				System.err.println("invalid parameter :" + sha
						+ " is not a hex-encoded SHA-1 prefix");
				System.exit(1);
			}
		} else {

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
			IOUtils.copy(file.getInputStream(), System.out);
		}

	}
}
