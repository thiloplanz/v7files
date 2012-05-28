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

import static v7db.files.CatCommand.decodeSHAPrefix;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentSHA;

import com.mongodb.DB;
import com.mongodb.MongoException;

class CopyCommand {

	private static ContentSHA findContentByPrefix(MongoContentStorage storage,
			String shaPrefix) throws DecoderException, IOException {
		return storage.findContentPointerByPrefix(decodeSHAPrefix(shaPrefix));
	}

	static V7File getParent(V7GridFS fs, String[] childPath)
			throws FileNotFoundException {
		V7File parent = fs.getFile(ArrayUtils.subarray(childPath, 0,
				childPath.length - 1));
		if (parent == null)
			throw new FileNotFoundException("target path "
					+ ArrayUtils.subarray(childPath, 0, childPath.length - 1));
		return parent;
	}

	private static void createFile(V7GridFS fs, ContentPointer content,
			String[] path, String contentType) throws IOException {
		V7File existing = fs.getFile(path);

		if (existing != null) {
			if (contentType == null)
				contentType = existing.getContentType();
			existing.setContent(content, contentType);
		} else {
			V7File parent = getParent(fs, path);
			parent.createChild(content, path[path.length - 1], contentType);
		}
	}

	static String[] getPath(String root, String path) {
		List<String> t = new ArrayList<String>();
		t.add(root);
		t.addAll(Arrays.asList(StringUtils.split(path, "/")));
		return t.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
	}

	public static void main(String[] args) throws MongoException, IOException {

		if (args.length != 5) {
			System.err.println("Copy files");
			System.err
					.println("  by name:   copy <sourceRoot> <sourcePath> <targetRoot> <targetPath>");
			System.err
					.println("  by hash:   copy -sha <shaHex> <targetRoot> <targetPath>");
			System.exit(1);
		}

		DB db = Configuration.getMongo().getDB(
				Configuration.getProperty("mongo.db"));

		V7GridFS fs = new V7GridFS(db);

		if ("-sha".equals(args[1])) {
			MongoContentStorage storage = new MongoContentStorage(db);
			String sha = args[2];
			try {
				ContentSHA file = findContentByPrefix(storage, sha);
				if (file == null)
					throw new FileNotFoundException("-sha " + sha);
				String[] path = getPath(args[3], args[4]);
				createFile(fs, file, path, null);
			} catch (DecoderException e) {
				throw new IllegalArgumentException("invalid parameter :" + sha
						+ " is not a hex-encoded SHA-1 prefix");
			}
		} else {
			String[] srcPath = getPath(args[1], args[2]);
			String[] targetPath = getPath(args[3], args[4]);
			V7File src = fs.getFile(srcPath);
			if (src == null) {
				throw new FileNotFoundException(args[1] + " " + args[2]);
			}
			if (src.hasContent()) {
				createFile(fs, src.getContentPointer(), targetPath, src
						.getContentType());
			} else {
				V7File existing = fs.getFile(targetPath);
				if (existing != null) {
					throw new IOException("copy target " + targetPath
							+ " already exists");
				}
				V7File parent = getParent(fs, targetPath);
				src.copyTo(parent.getId(), targetPath[targetPath.length - 1]);
			}

		}

	}

}
