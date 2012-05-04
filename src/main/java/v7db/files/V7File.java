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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.bson.BSONObject;

import v7db.files.spi.Content;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentSHA;
import v7db.files.spi.InlineContent;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class V7File {

	// lazy-loaded
	private Content gridFile;

	private final V7GridFS gridFS;

	private final DBObject metaData;

	private final V7File parent;

	V7File(V7GridFS gridFS, DBObject metaData, V7File parent) {
		this.gridFS = gridFS;
		this.metaData = metaData;
		this.parent = parent;
	}

	static V7File lazy(V7GridFS gridFS, Object id, V7File parent) {
		return new V7File(gridFS, new BasicDBObject("_id", id), parent);
	}

	private void loadGridFile() throws IOException {
		if (gridFile == null)
			gridFile = gridFS.getContent(metaData);
	}

	public String getContentType() {
		Object x = metaData.get("contentType");
		if (x instanceof String)
			return (String) x;
		return null;
	}

	public Object getId() {
		return metaData.get("_id");
	}

	public Object getParentId() {
		if (parent != null)
			return parent.getId();
		return metaData.get("parent");
	}

	public int getVersion() {
		return BSONUtils.getRequiredInt(metaData, "_version");
	}

	public V7File getParent() {
		return parent;
	}

	public String getName() {
		Object o = metaData.get("filename");
		if (o instanceof String)
			return (String) o;
		return null;
	}

	/**
	 * useful to send gzipped contents directly to a client that supports it,
	 * without having to uncompress it first.
	 * 
	 * @return the _compressed_ data (using gzip), if present, null if not (no
	 *         on-the-fly compression is done)
	 * 
	 */

	public InputStream getInputStreamWithGzipContents() throws IOException {
		// loadGridFile();
		// String store = (String) gridFile.get("store");
		// if ("gz".equals(store))
		// return gridFile.getInputStream();
		return null;
	}

	/**
	 * 
	 * @return null, if the file is not stored using gzip
	 */
	public Long getGZipLength() {
		// loadGridFile();
		// String store = (String) gridFile.get("store");
		// if ("gz".equals(store))
		// return gridFile.getLength();
		return null;
	}

	/**
	 * takes care of de-compression
	 * 
	 * @return an InputStream to _uncompressed_ data
	 * @throws IOException
	 */
	public InputStream getInputStream() throws IOException {
		loadGridFile();

		if (gridFile == null)
			return null;
		try {
			return gridFile.getInputStream();
		} catch (IllegalArgumentException e) {
			throw new IOException(e.getMessage() + " on file " + getName());
		}

	}

	public ContentPointer getContentPointer() {
		return gridFS.getContentPointer(metaData);
	}

	public boolean hasContent() {
		return getContentPointer() != null;
	}

	public Long getLength() {
		ContentPointer p = getContentPointer();
		if (p == null)
			return null;
		return p.getLength();
	}

	public String getDigest() {
		ContentPointer contentPointer = getContentPointer();
		if (contentPointer instanceof ContentSHA) {
			return ((ContentSHA) contentPointer).getDigest();
		}
		if (contentPointer instanceof InlineContent) {
			try {
				return DigestUtils.shaHex(((InlineContent) contentPointer)
						.getInputStream());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		// TODO:
		System.err.println("NO DIGEST!");
		return null;
	}

	public List<V7File> getChildren() {
		return gridFS.getChildren(this);
	}

	public V7File getChild(String childName) {
		return gridFS.getChild(this, childName);
	}

	public V7File createChild(byte[] data, String filename, String contentType)
			throws IOException {
		Object childId = gridFS.addFile(data, getId(), filename, contentType);
		return lazy(gridFS, childId, this);
	}

	public V7File createChild(ContentPointer data, String filename,
			String contentType) throws IOException {
		Object childId = gridFS.addFile(data, getId(), filename, contentType);
		return lazy(gridFS, childId, this);
	}

	public V7File createChild(byte[] data, int offset, int len,
			String filename, String contentType) throws IOException {
		Object childId = gridFS.addFile(data, offset, len, getId(), filename,
				contentType);
		return lazy(gridFS, childId, this);
	};

	public V7File createChild(File data, String filename, String contentType)
			throws IOException {
		Object childId = gridFS.addFile(data, getId(), filename, contentType);
		return lazy(gridFS, childId, this);
	}

	public V7File createChild(InputStream data, long size, String filename,
			String contentType) throws IOException {
		if (size <= 1024 * 1024)
			return createChild(IOUtils.toByteArray(data, size), filename,
					contentType);

		File temp = File.createTempFile("v7files-upload-", ".tmp");
		try {
			FileUtils.copyInputStreamToFile(data, temp);
			if (temp.length() != size) {
				throw new IOException("read incorrect number of bytes for "
						+ filename + ", expected " + size + " but got "
						+ temp.length());
			}
			return createChild(temp, filename, contentType);

		} finally {
			temp.delete();
		}
	}

	public V7File createChild(InputStream data, String filename,
			String contentType) throws IOException {
		if (data == null)
			return createChild(null, 0, 0, filename, contentType);
		// read the first megabyte into a buffer
		byte[] buffer = new byte[1024 * 1024];
		int read = V7GridFS.readFully(data, buffer);
		if (read == 0)
			return createChild(ArrayUtils.EMPTY_BYTE_ARRAY, filename,
					contentType);
		// did it fit completely in the buffer?
		if (read < buffer.length)
			return createChild(buffer, 0, read, filename, contentType);
		// if not, copy to a temporary file
		// so that we can calculate the SHA-1 first
		File temp = File.createTempFile("v7files-upload-", ".tmp");
		try {
			OutputStream out = new FileOutputStream(temp);
			out.write(buffer);
			buffer = null;
			IOUtils.copy(data, out);
			out.close();
			return createChild(temp, filename, contentType);
		} finally {
			temp.delete();
		}
	}

	public void rename(String newName) throws IOException {
		metaData.put("filename", newName);
		gridFS.updateMetaData(metaData);
	}

	public void moveTo(Object newParentId, String newName) throws IOException {
		metaData.put("parent", newParentId);
		rename(newName);
	}

	public void setContent(ContentPointer data, String contentType)
			throws IOException {
		metaData.put("contentType", contentType);
		gridFS.updateContents(metaData, data);
	}

	public void setContent(byte[] data, String contentType) throws IOException {
		metaData.put("contentType", contentType);
		gridFS.updateContents(metaData, data);
	}

	public void setContent(InputStream data, String contentType)
			throws IOException {
		metaData.put("contentType", contentType);
		gridFS.updateContents(metaData, data, null);
	}

	public void setContent(InputStream data, long size, String contentType)
			throws IOException {
		metaData.put("contentType", contentType);
		gridFS.updateContents(metaData, data, size);
	}

	public Date getModifiedDate() {
		return (Date) metaData.get("updated_at");
	}

	public Date getCreateDate() {
		return (Date) metaData.get("created_at");
	}

	public void delete() throws IOException {
		gridFS.delete(this);
	}

	/**
	 * @param permission
	 *            "read", "write", or "open"
	 * @return the ACL for this permission, if not set, inherited from parents
	 *         null if not set (not even at parents), empty if set but empty
	 */
	public Object[] getEffectiveAcl(String permission) {
		BSONObject acls = (BSONObject) metaData.get("acl");
		if (acls == null)
			if (parent != null)
				return parent.getEffectiveAcl(permission);
			else
				return null;
		List<?> acl = (List<?>) acls.get(permission);
		if (acl == null)
			return ArrayUtils.EMPTY_OBJECT_ARRAY;
		return acl.toArray();
	}

	Object[] getAcl(String permission) {
		BSONObject acls = (BSONObject) metaData.get("acl");
		if (acls == null)
			return null;
		List<?> acl = (List<?>) acls.get(permission);
		if (acl == null)
			return ArrayUtils.EMPTY_OBJECT_ARRAY;
		return acl.toArray();
	}

}
