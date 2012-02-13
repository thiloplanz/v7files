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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class V7GridFS {

	private static final String bucket = "v7files";

	/**
	 * very small files are inlined (no chunks) we have no performance data yet
	 * about what "small" should be, but probably very small in order not to
	 * clutter the files collection
	 * 
	 * https://github.com/thiloplanz/v7files/wiki/Compression
	 * https://jira.mongodb.org/browse/SERVER-4409
	 */
	private static final int inlineSize = 256;

	private final DBCollection files;

	private final GridFS fs;

	public V7GridFS(DB db) {
		files = db.getCollection(bucket);
		fs = new GridFS(db, "v7.fs");
	}

	public static V7GridFS getIfExists(Mongo mongo, String dbName) {
		return mongo.getDB(dbName).collectionExists(bucket) ? new V7GridFS(
				mongo.getDB(dbName)) : null;
	}

	public V7File getFile(String... path) {

		// the filesystem root
		V7File parentFile = V7File.lazy(this, path[0]);

		if (path.length == 1) {
			return parentFile;
		}

		DBObject metaData;
		// directly under the root
		if (path.length == 2) {
			metaData = files.findOne(new BasicDBObject("parent", path[0])
					.append("filename", path[1]));
		}

		else {
			List<String> filenames = Arrays.asList(path)
					.subList(1, path.length);
			List<DBObject> candidates = files.find(
					new BasicDBObject("filename", new BasicDBObject("$in",
							filenames))).toArray();
			// we need to have at least one candidate for every path component
			if (candidates.size() < filenames.size())
				return null;

			Object parent = path[0];

			metaData = null;
			path: for (String fileName : filenames) {
				for (DBObject c : candidates) {
					if (parent.equals(c.get("parent"))
							&& fileName.equals(c.get("filename"))) {
						parent = c.get("_id");
						metaData = c;
						parentFile = new V7File(this, metaData, parentFile);
						continue path;
					}
				}
				return null;
			}
		}

		if (metaData == null)
			return null;
		return new V7File(this, metaData, parentFile);
	}

	GridFSDBFile findContent(byte[] sha) {
		return fs.findOne(new BasicDBObject("_id", sha));
	}

	InputStream readContent(byte[] sha) throws IOException {
		GridFSDBFile f = findContent(sha);
		if (f == null)
			return null;
		return new V7File(f, this).getInputStream();
	}

	public boolean contentAlreadyExists(byte[] sha) {
		return null != findContent(sha);
	}

	/**
	 * @param data
	 *            can be null, for a file without content (e.g. a folder)
	 * @param parentFileId
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public Object addFile(byte[] data, Object parentFileId, String filename,
			String contentType) throws IOException {
		if (data == null)
			return addFile(null, 0, 0, parentFileId, filename, contentType);
		return addFile(data, 0, data.length, parentFileId, filename,
				contentType);
	}

	public Object addFile(byte[] data, int offset, int len,
			Object parentFileId, String filename, String contentType)
			throws IOException {
		BasicDBObject metaData = new BasicDBObject("filename", filename)
				.append("parent", parentFileId).append("_id", new ObjectId());

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		if (data != null) {
			// for up to 31 bytes, storing the complete file inline
			// takes less space than just storing the SHA-1 and length
			// 20 (SHA-1) + 1 (sha - in) + 6 (length) + 4 (int32)
			if (len < 32) {
				metaData.append("in", ArrayUtils.subarray(data, offset, offset
						+ len));
			} else {
				byte[] sha = insertContents(data, offset, len, filename,
						metaData.get("_id"), contentType);
				metaData.append("sha", sha).append("length", len);
			}
		}

		insertMetaData(metaData);
		return metaData.get("_id");
	}

	public Object addFile(File data, Object parentFileId, String filename,
			String contentType) throws IOException {
		// avoid temporary files for small data
		if (data != null && data.length() < 32 * 1024) {
			byte[] smallData = FileUtils.readFileToByteArray(data);
			return addFile(smallData, 0, smallData.length, parentFileId,
					filename, contentType);
		}
		BasicDBObject metaData = new BasicDBObject("filename", filename)
				.append("parent", parentFileId).append("_id", new ObjectId());

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		if (data != null) {
			byte[] sha = insertContents(data, filename, metaData.get("_id"),
					contentType);
			metaData.append("sha", sha).append("length", data.length());
		}

		insertMetaData(metaData);
		return metaData.get("_id");
	}

	public List<V7File> getChildren(V7File parent) {
		List<V7File> children = new ArrayList<V7File>();
		for (DBObject child : files.find(new BasicDBObject("parent", parent
				.getId()))) {
			children.add(new V7File(this, child, parent));
		}
		return children;
	}

	private void insertInlineContents(byte[] data, String store, byte[] sha,
			String filename, Object fileId, String contentType) {
		GridFSInputFile file = fs.createFile(ArrayUtils.EMPTY_BYTE_ARRAY);
		file.setFilename(filename);
		file.setContentType(contentType);
		file.put("_id", sha);
		file.put("refs", new Object[] { fileId });
		file.put("refHistory", file.get("refs"));
		file.put("store", store);
		file.put("in", data);
		file.save();
	}

	private void insertGzipContents(InputStream deflatedData, byte[] sha,
			String filename, Object fileId, String contentType) {
		GridFSInputFile file = fs.createFile(deflatedData, true);
		file.setFilename(filename + ".gz");
		file.setContentType(contentType);
		file.put("_id", sha);
		file.put("refs", new Object[] { fileId });
		file.put("refHistory", file.get("refs"));
		file.put("store", "gz");
		file.save();
	}

	private boolean contains(List<?> list, BSONObject bson) {
		if (list == null || list.isEmpty())
			return false;
		for (Object o : list) {
			if (o instanceof BSONObject) {
				BSONObject b = (BSONObject) o;
				if (b.keySet().equals(bson.keySet())
						&& Arrays.equals(BSON.encode(b), BSON.encode(bson)))
					return true;
			}
		}
		return false;
	}

	private void checkRefBase(byte[] sha, BSONObject alt) {
		// check if the "base" contents exist and add "refBase"
		List<?> bases = (List<?>) alt.get("base");
		if (bases != null) {
			DBCollection bc = getGridFSMetaCollection();
			for (Object base : bases) {

				if (base instanceof BSONObject) {
					Object id = ((BSONObject) base).get("_id");
					long check = bc.count(new BasicDBObject("_id", id));
					if (check == 0)
						throw new IllegalArgumentException(
								"base does not exist:"
										+ Arrays
												.deepToString(new Object[] { id }));
					bc.update(new BasicDBObject("_id", id), new BasicDBObject(
							"$addToSet", new BasicDBObject("refBase", sha)));
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

			getGridFSMetaCollection().insert(x);
		} else {
			// check if the same alt is already there
			List<?> existingAlt = (List<?>) existing.get("alt");
			if (contains(existingAlt, alt)) {
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

	private void putRefs(BSONObject newFile, Object fileId) {
		newFile.put("refs", new Object[] { fileId });
		newFile.put("refHistory", newFile.get("refs"));
	}

	private byte[] insertContents(byte[] data, int offset, int len,
			String filename, Object fileId, String contentType)
			throws IOException {
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

	private byte[] insertContents(File data, String filename, Object fileId,
			String contentType) throws IOException {

		// avoid temporary files for small data
		if (data.length() < 32 * 1024) {
			byte[] smallData = FileUtils.readFileToByteArray(data);
			return insertContents(smallData, 0, smallData.length, filename,
					fileId, contentType);
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

	private void insertMetaData(DBObject metaData) throws IOException {
		metaData.put("_version", 1);
		metaData.put("created_at", new Date());
		WriteResult result = files.insert(metaData);
		String error = result.getError();
		if (error != null)
			throw new IOException(error);
	}

	void updateMetaData(DBObject metaData) throws IOException {
		metaData.put("updated_at", new Date());
		try {
			Vermongo.update(files, metaData);
		} catch (UpdateConflictException e) {
			throw new IOException(e);
		}

	}

	private DBCollection getGridFSMetaCollection() {
		return files.getDB().getCollectionFromString("v7.fs.files");
	}

	// add a backreference to the GridFS file (for garbage collection)
	private void addRefs(byte[] sha, Object fileId) {
		if (fileId != null)
			getGridFSMetaCollection().update(
					new BasicDBObject("_id", sha),
					new BasicDBObject("$addToSet", new BasicDBObject("refs",
							fileId).append("refHistory", fileId)));
	}

	private void addRefsAndAlt(byte[] sha, Object fileId, BSONObject alt) {
		BasicDBObject addToSet = new BasicDBObject("alt", alt);
		if (fileId != null)
			addToSet.append("refs", fileId).append("refHistory", fileId);
		if (fileId != null)
			getGridFSMetaCollection().update(new BasicDBObject("_id", sha),
					new BasicDBObject("$addToSet", addToSet));

	}

	// remove the current backreference (but keep the refHistory)
	private void removeRef(byte[] sha, Object fileId) {
		if (sha == null)
			return;
		getGridFSMetaCollection().update(new BasicDBObject("_id", sha),
				new BasicDBObject("$pull", new BasicDBObject("refs", fileId)));
	}

	void updateContents(DBObject metaData, byte[] contents) throws IOException {
		updateContents(metaData, contents, 0, contents == null ? 0
				: contents.length);
	}

	/**
	 * read into the buffer, continuing until the stream is finished or the
	 * buffer is full.
	 * 
	 * @return the number of bytes read, which could be 0 (not -1)
	 * @throws IOException
	 */
	static int readFully(InputStream data, byte[] buffer) throws IOException {
		int read = data.read(buffer);
		if (read == -1) {
			return 0;
		}
		while (read < buffer.length) {
			int added = data.read(buffer, read, buffer.length - read);
			if (added == -1)
				return read;
			read += added;
		}
		return read;
	}

	void updateContents(DBObject metaData, InputStream contents, Long size)
			throws IOException {
		if (contents == null) {
			updateContents(metaData, (byte[]) null);
			return;
		}
		if (size != null) {
			if (size <= 1024 * 1024) {
				updateContents(metaData, IOUtils.toByteArray(contents, size));
				return;
			}
			File temp = File.createTempFile("v7files-upload-", ".tmp");
			try {
				FileUtils.copyInputStreamToFile(contents, temp);
				if (temp.length() != size)
					throw new IOException("read incorrect number of bytes for "
							+ metaData.get("filename") + ", expected " + size
							+ " but got " + temp.length());
				updateContents(metaData, temp);
			} finally {
				temp.delete();
			}
			return;
		}

		// read the first megabyte into a buffer
		byte[] buffer = new byte[1024 * 1024];
		int read = readFully(contents, buffer);
		if (read == 0) {
			updateContents(metaData, ArrayUtils.EMPTY_BYTE_ARRAY);
			return;
		}

		// did it fit completely in the buffer?
		if (read < buffer.length) {
			updateContents(metaData, buffer, 0, read);
			return;
		}
		// if not, copy to a temporary file
		// so that we can calculate the SHA-1 first
		File temp = File.createTempFile("v7files-upload-", ".tmp");
		try {
			OutputStream out = new FileOutputStream(temp);
			out.write(buffer);
			buffer = null;
			IOUtils.copy(contents, out);
			out.close();
			updateContents(metaData, temp);
		} finally {
			temp.delete();
		}
	}

	private void updateContents(DBObject metaData, File contents)
			throws IOException {

		byte[] oldSha = (byte[]) metaData.get("sha");

		byte[] sha = insertContents(contents,
				(String) metaData.get("filename"), metaData.get("_id"),
				(String) metaData.get("contentType"));

		if (oldSha != null && Arrays.equals(sha, oldSha))
			return;

		metaData.put("sha", sha);
		metaData.put("length", contents.length());
		updateMetaData(metaData);
		removeRef(oldSha, metaData.get("_id"));
	}

	private void updateContents(DBObject metaData, byte[] contents, int offset,
			int len) throws IOException {

		byte[] oldSha = (byte[]) metaData.get("sha");
		// for up to 31 bytes, storing the complete file inline
		// takes less space than just storing the SHA-1 and length
		// 20 (SHA-1) + 1 (sha - in) + 6 (length) + 4 (int32)
		if (contents != null && len < 32) {
			byte[] smallData = ArrayUtils.subarray(contents, offset, offset
					+ len);
			byte[] oldSmallData = (byte[]) metaData.get("in");
			if (oldSmallData != null && Arrays.equals(oldSmallData, smallData))
				return;

			metaData.put("in", smallData);
			metaData.removeField("sha");
			metaData.removeField("length");
			updateMetaData(metaData);
			removeRef(oldSha, metaData.get("_id"));
			return;
		}

		byte[] sha = insertContents(contents, offset, len, (String) metaData
				.get("filename"), metaData.get("_id"), (String) metaData
				.get("contentType"));

		if (oldSha != null && Arrays.equals(sha, oldSha))
			return;

		metaData.put("sha", sha);
		metaData.put("length", contents.length);
		updateMetaData(metaData);
		removeRef(oldSha, metaData.get("_id"));
	}

	public V7File getChild(V7File parentFile, String childName) {
		DBObject child = files.findOne(new BasicDBObject("parent", parentFile
				.getId()).append("filename", childName));
		if (child == null)
			return null;
		return new V7File(this, child, parentFile);
	}

	void delete(V7File file) throws IOException {
		// TODO: should check the version present in the db
		byte[] oldSha = file.getSha();
		Vermongo.remove(files, file.getId(), new BasicDBObject("deleted_at",
				new Date()));
		removeRef(oldSha, file.getId());
	}

}
