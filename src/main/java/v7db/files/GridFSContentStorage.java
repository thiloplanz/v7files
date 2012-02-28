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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

class GridFSContentStorage {

	/**
	 * very small files are inlined (no chunks) we have no performance data yet
	 * about what "small" should be, but probably very small in order not to
	 * clutter the files collection
	 * 
	 * https://github.com/thiloplanz/v7files/wiki/Compression
	 * https://jira.mongodb.org/browse/SERVER-4409
	 */
	private static final int inlineSize = 256;

	private final GridFS fs;

	private final DBCollection metaCollection;

	GridFSContentStorage(GridFS fs) {
		this.fs = fs;
		this.metaCollection = fs.getDB().getCollectionFromString(
				fs.getBucketName() + ".files");
	}

	GridFSDBFile findContent(byte[] sha) {
		return fs.findOne(new BasicDBObject("_id", sha));
	}

	GridFSDBFile findContentByPrefix(byte[] sha) {
		if (sha.length == 20)
			return findContent(sha);

		if (sha.length > 20)
			throw new IllegalArgumentException();

		byte[] lower = Arrays.copyOf(sha, 20); // 0-padded
		byte[] higher = Arrays.copyOf(sha, 20); // FF-padded
		for (int i = sha.length; i < higher.length; i++) {
			higher[i] = (byte) 0xFF;
		}

		List<DBObject> files = metaCollection.find(
				QueryUtils.between("_id", lower, higher), new BasicDBObject())
				.limit(2).toArray();
		if (files.isEmpty())
			return null;
		if (files.size() == 1)
			return findContent((byte[]) files.get(0).get("_id"));
		throw new IllegalArgumentException(Hex.encodeHexString(sha)
				+ " is not a unique SHA prefix");

	}

	InputStream readContent(byte[] sha) throws IOException {
		GridFSDBFile f = findContent(sha);
		if (f == null)
			return null;
		return new V7File(f, this).getInputStream();
	}

	InputStream readContent(byte[] sha, int off, int len) throws IOException {
		GridFSDBFile f = findContent(sha);
		if (f == null)
			return null;
		return new V7File(f, this).getInputStream(off, len);
	}

	InputStream readContent(GridFSDBFile f) throws IOException {
		if (f == null)
			return null;
		return new V7File(f, this).getInputStream();
	}

	InputStream readContent(GridFSDBFile f, Integer off, Integer len)
			throws IOException {
		if (f == null)
			return null;
		return new V7File(f, this).getInputStream(off, len);
	}

	public boolean contentAlreadyExists(byte[] sha) {
		return null != findContent(sha);
	}

	private void insertInlineContents(byte[] data, String store, byte[] sha,
			String filename, Object fileId, String contentType) {

		BasicDBObject file = new BasicDBObject();
		if (StringUtils.isNotBlank(filename))
			file.put("filename", filename);
		if (StringUtils.isNotBlank(contentType))
			file.put("contentType", contentType);
		file.put("_id", sha);
		putRefs(file, fileId);
		file.put("store", store);
		file.put("in", data);
		metaCollection.insert(file);
	}

	private void insertGzipContents(InputStream deflatedData, byte[] sha,
			String filename, Object fileId, String contentType) {
		GridFSInputFile file = fs.createFile(deflatedData, true);
		file.setFilename(filename + ".gz");
		file.setContentType(contentType);
		file.put("_id", sha);
		putRefs(file, fileId);
		file.put("store", "gz");
		file.save();
	}

	private void checkRefBase(byte[] sha, BSONObject alt) {
		// check if the "base" contents exist and add "refBase"
		List<?> bases = (List<?>) alt.get("base");
		if (bases != null) {
			for (Object base : bases) {

				if (base instanceof BSONObject) {
					Object id = ((BSONObject) base).get("_id");
					long check = metaCollection.count(new BasicDBObject("_id",
							id));
					if (check == 0)
						throw new IllegalArgumentException(
								"base does not exist:"
										+ Arrays
												.deepToString(new Object[] { id }));
					metaCollection.update(new BasicDBObject("_id", id),
							new BasicDBObject("$addToSet", new BasicDBObject(
									"refBase", sha)));
				}

			}
		}

	}

	void registerAlt(byte[] sha, BSONObject alt, String filename,
			Object fileId, String contentType) {
		if (!alt.containsField("store"))
			throw new IllegalArgumentException(
					"must specify storage method in `store` field");
		GridFSDBFile existing = findContent(sha);
		if (existing == null) {
			checkRefBase(sha, alt);
			BasicDBObject x = new BasicDBObject();
			x.put("_id", sha);
			x.put("store", "alt");
			x.put("alt", Arrays.asList(alt));
			if (StringUtils.isNotBlank(filename))
				x.put("filename", filename);
			if (StringUtils.isNotBlank(contentType))
				x.put("contentType", contentType);
			if (fileId != null)
				putRefs(x, fileId);

			metaCollection.insert(x);
		} else {
			// check if the same alt is already there
			if (BSONUtils.contains(existing, "alt", alt)) {
				addRefs(sha, fileId);
				return;
			}
			// we don't check the ref-bases, because
			// now this "alt" is optional
			// when it gets "upgraded" to the primary
			// representation, then we check them
			addRefsAndAlt(sha, fileId, alt);

		}
	}

	byte[] insertContents(byte[] data, int offset, int len, String filename,
			Object fileId, String contentType) throws IOException {
		byte[] sha = DigestUtils
				.sha(new ByteArrayInputStream(data, offset, len));

		if (!contentAlreadyExists(sha)) {
			// try to deflate first to get the data inline
			int clen = -1;
			if (len < inlineSize * 20) {
				byte[] compressed = new byte[len];
				clen = Compression.deflate(data, offset, len, compressed);
				if (clen > 0) {
					// inline
					if (clen <= inlineSize) {
						insertInlineContents(ArrayUtils.subarray(compressed, 0,
								clen), "zin", sha, filename + ".z", fileId,
								contentType);
						return sha;
					}
				}
				compressed = null;
			}

			if (clen == -1
					|| (clen > 0 && clen + Compression.GZIP_STORAGE_OVERHEAD < len)) {
				// could not be inlined, but GZIP might work
				byte[] compressed = Compression.gzip(data, offset, len);
				if (compressed != null) {
					insertGzipContents(new ByteArrayInputStream(compressed),
							sha, filename, fileId, contentType);
					return sha;
				}
				compressed = null;
			}

			if (len <= inlineSize) {
				insertInlineContents(ArrayUtils.subarray(data, offset, offset
						+ len), "in", sha, filename, fileId, contentType);
				return sha;
			}

			GridFSInputFile file = fs.createFile(new ByteArrayInputStream(data,
					offset, len), true);
			file.setFilename(filename);
			file.setContentType(contentType);
			file.put("_id", sha);
			putRefs(file, fileId);
			file.save();
		} else {
			addRefs(sha, fileId);

		}
		return sha;
	}

	byte[] insertContents(File data, String filename, Object fileId,
			String contentType) throws IOException {

		// avoid temporary files for small data
		if (data.length() < 32 * 1024) {
			byte[] smallData = FileUtils.readFileToByteArray(data);
			return insertContents(smallData, filename, fileId, contentType);
		}

		FileInputStream fis = new FileInputStream(data);
		byte[] sha = DigestUtils.sha(fis);
		fis.close();

		if (!contentAlreadyExists(sha)) {
			final File compressed = Compression.gzip(data);
			if (compressed != null) {
				try {
					insertGzipContents(new FileInputStream(compressed), sha,
							filename, fileId, contentType);
					return sha;
				} finally {
					compressed.delete();
				}
			}
			GridFSInputFile file = fs.createFile(data);
			file.setFilename(filename);
			file.setContentType(contentType);
			file.put("_id", sha);
			file.put("refs", new Object[] { fileId });
			file.put("refHistory", file.get("refs"));
			file.save();
		} else {
			addRefs(sha, fileId);

		}
		return sha;
	}

	byte[] insertContents(byte[] data, String filename, Object fileId,
			String contentType) throws IOException {
		return insertContents(data, 0, data.length, filename, fileId,
				contentType);
	}

	// add a backreference to the GridFS file (for garbage collection)
	private void addRefs(byte[] sha, Object fileId) {
		if (fileId != null)
			metaCollection.update(new BasicDBObject("_id", sha),
					new BasicDBObject("$addToSet", new BasicDBObject("refs",
							fileId).append("refHistory", fileId)));
	}

	private void addRefsAndAlt(byte[] sha, Object fileId, BSONObject alt) {
		BasicDBObject addToSet = new BasicDBObject("alt", alt);
		if (fileId != null)
			addToSet.append("refs", fileId).append("refHistory", fileId);
		if (fileId != null)
			metaCollection.update(new BasicDBObject("_id", sha),
					new BasicDBObject("$addToSet", addToSet));

	}

	// remove the current backreference (but keep the refHistory)
	void removeRef(byte[] sha, Object fileId) {
		if (sha == null)
			return;
		metaCollection.update(new BasicDBObject("_id", sha), new BasicDBObject(
				"$pull", new BasicDBObject("refs", fileId)));
	}

	private void putRefs(BSONObject newFile, Object fileId) {
		if (fileId != null) {
			newFile.put("refs", new Object[] { fileId });
			newFile.put("refHistory", newFile.get("refs"));
		}
	}
}
