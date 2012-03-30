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
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import v7db.files.aws.GridFSContentStorageWithS3;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class GridFSContentStorage {

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

	protected GridFSContentStorage(DB db) {
		this.fs = new GridFS(db, "v7.fs");
		this.metaCollection = fs.getDB().getCollectionFromString(
				fs.getBucketName() + ".files");
	}

	public static GridFSContentStorage configure(Mongo mongo, Properties props) {
		if (StringUtils.isNotBlank(props.getProperty("s3.accessKey"))
				&& StringUtils.isNotBlank(props.getProperty("s3.bucket")))
			return GridFSContentStorageWithS3.configure(mongo, props);

		return new GridFSContentStorage(mongo.getDB(props
				.getProperty("mongo.db")));
	}

	protected GridFSDBFile findContent(byte[] sha) {
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

	protected void registerAlt(byte[] sha, BSONObject alt, String filename,
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

	private byte[] insertContents(byte[] data, String filename, Object fileId,
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

		metaCollection.update(new BasicDBObject("_id", sha), new BasicDBObject(
				"$addToSet", addToSet));

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

	public static byte[] getSha(BSONObject metaData) {
		byte[] sha = (byte[]) metaData.get("sha");
		if (sha != null)
			return sha;
		byte[] data = getInlineData(metaData);
		if (data != null) {
			return DigestUtils.sha(data);
		}
		return null;
	}

	static String getDigest(BSONObject metaData) {
		byte[] sha = getSha(metaData);
		if (sha == null)
			return null;
		return Hex.encodeHexString(sha);
	}

	static byte[] getInlineData(BSONObject metaData) {
		return (byte[]) metaData.get("in");
	}

	public static Long getLength(BSONObject metaData) {
		byte[] inline = getInlineData(metaData);
		if (inline != null)
			return Long.valueOf(inline.length);
		return BSONUtils.getLong(metaData, "length");
	}

	public static String getFilename(BSONObject metaData) {
		return BSONUtils.getString(metaData, "filename");
	}

	public static String getContentType(BSONObject metaData) {
		return BSONUtils.getString(metaData, "contentType");
	}

	public InputStream getInputStream(BSONObject file) throws IOException {
		byte[] inline = getInlineData(file);
		if (inline != null)
			return new ByteArrayInputStream(inline);
		byte[] sha = getSha(file);
		if (sha == null)
			return null;

		GridFSDBFile gridFile = findContent(sha);
		try {
			return getInputStream(gridFile);
		} catch (IllegalArgumentException e) {
			Object name = file.get("filename");
			if (!(name instanceof String))
				name = Hex.encodeHexString(sha);
			throw new IOException(e.getMessage() + " on file " + name);
		}
	}

	public InputStream getInputStreamWithGzipContents(BSONObject file)
			throws IOException {
		byte[] inline = getInlineData(file);
		if (inline != null)
			return null;
		byte[] sha = getSha(file);
		if (sha == null)
			return null;
		GridFSDBFile gridFile = findContent(sha);
		String store = (String) gridFile.get("store");
		if ("gz".equals(store))
			return gridFile.getInputStream();
		return null;
	}

	InputStream getInputStream(GridFSDBFile gridFile) throws IOException,
			IllegalArgumentException {
		String store = (String) gridFile.get("store");
		if (store == null || "raw".equals(store))
			return gridFile.getInputStream();
		if ("z".equals(store))
			return new InflaterInputStream(gridFile.getInputStream(),
					new Inflater(true));
		if ("zin".equals(store))
			return new InflaterInputStream(new ByteArrayInputStream(
					(byte[]) gridFile.get("in")), new Inflater(true));
		if ("in".equals(store))
			return new ByteArrayInputStream((byte[]) gridFile.get("in"));
		if ("gz".equals(store))
			return new GZIPInputStream(gridFile.getInputStream());
		if ("alt".equals(store))
			return OutOfBand.getInputStream(this, gridFile);
		if ("zip".equals(store))
			return Compression.unzip(gridFile.getInputStream());
		throw new IllegalArgumentException("unsupported storage scheme '"
				+ store + "'");
	}

	/**
	 * inserts the contents, and returns metadata describing it (with either
	 * "sha" and "length" fields or inline data "in"). Does not save the
	 * metadata, or make back references.
	 * 
	 */

	public final BSONObject insertContents(byte[] data, int inlineUntil,
			String filename, String contentType) throws IOException {
		return insertContentsAndBackRefs(data, null, inlineUntil, filename,
				contentType);
	}

	public final BSONObject insertContents(byte[] data, int offset, int len,
			int inlineUntil, String filename, String contentType)
			throws IOException {
		return insertContentsAndBackRefs(data, offset, len, null, inlineUntil,
				filename, contentType);
	}

	public final BSONObject insertContents(File data, int inlineUntil,
			String filename, String contentType) throws IOException {
		return insertContentsAndBackRefs(data, null, inlineUntil, filename,
				contentType);
	}

	final BSONObject insertContentsAndBackRefs(byte[] data, Object fileId,
			int inlineUntil, String filename, String contentType)
			throws IOException {
		if (data != null)
			return insertContentsAndBackRefs(data, 0, data.length, fileId,
					inlineUntil, filename, contentType);
		return insertContentsAndBackRefs(null, 0, 0, fileId, inlineUntil,
				filename, contentType);
	}

	protected BSONObject insertContentsAndBackRefs(byte[] data, int offset,
			int len, Object fileId, int inlineUntil, String filename,
			String contentType) throws IOException {
		BasicBSONObject metaData = new BasicBSONObject();

		if (StringUtils.isNotBlank(filename))
			metaData.append("filename", filename);

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		if (data != null) {
			if (len <= inlineUntil) {
				metaData.append("in", ArrayUtils.subarray(data, offset, offset
						+ len));
			} else {
				byte[] sha = insertContents(data, offset, len, filename,
						fileId, contentType);
				metaData.append("sha", sha).append("length", len);
			}
		}

		return metaData;
	}

	protected BSONObject insertContentsAndBackRefs(File data, Object fileId,
			int inlineUntil, String filename, String contentType)
			throws IOException {
		if (data == null)
			return insertContentsAndBackRefs(null, 0, 0, fileId, inlineUntil,
					filename, contentType);
		long len = data.length();
		if (data.length() <= inlineUntil)
			return insertContentsAndBackRefs(FileUtils
					.readFileToByteArray(data), fileId, inlineUntil, filename,
					contentType);

		BasicBSONObject metaData = new BasicBSONObject();

		if (StringUtils.isNotBlank(filename))
			metaData.append("filename", filename);

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		byte[] sha = insertContents(data, filename, fileId, contentType);
		metaData.append("sha", sha);
		BSONUtils.putIntegerOrLong(metaData, "length", len);

		return metaData;

	}

}
