/**
 * Copyright (c) 2011, Thilo Planz. All rights reserved.
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
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.binary.Hex;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

public class V7File {

	// lazy-loaded
	private GridFSDBFile gridFile;

	private final V7GridFS gridFS;

	private final DBObject metaData;

	V7File(V7GridFS gridFS, DBObject metaData) {
		this.gridFS = gridFS;
		this.metaData = metaData;
	}

	static V7File lazy(V7GridFS gridFS, Object id) {
		return new V7File(gridFS, new BasicDBObject("_id", id));
	}

	private void loadGridFile() {
		if (gridFile == null)
			gridFile = gridFS.findContent(getSha());
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
		return metaData.get("parent");
	}

	public String getName() {
		Object o = metaData.get("filename");
		if (o instanceof String)
			return (String) o;
		return null;
	}

	public InputStream getInputStream() {
		if (getSha() == null)
			return null;
		loadGridFile();
		return gridFile.getInputStream();
	}

	private byte[] getSha() {
		return (byte[]) metaData.get("sha");
	}

	public boolean hasContent() {
		return getSha() != null;
	}

	public Long getLength() {
		Object l = metaData.get("length");
		if (l instanceof Long)
			return (Long) l;
		if (l instanceof Number)
			return ((Number) l).longValue();
		return null;
	}

	public String getDigest() {
		byte[] sha = getSha();
		if (sha == null)
			return null;
		return Hex.encodeHexString(sha);
	}

	public List<V7File> getChildren() {
		return gridFS.getChildren(getId());
	}

	public V7File getChild(String childName) {
		return gridFS.getChild(getId(), childName);
	}

	public V7File createChild(byte[] data, String filename, String contentType)
			throws IOException {
		Object childId = gridFS.addFile(data, getId(), filename, contentType);
		return lazy(gridFS, childId);
	}

	public void rename(String newName) throws IOException {
		metaData.put("filename", newName);
		gridFS.updateMetaData(metaData);
	}

	public void moveTo(Object newParentId, String newName) throws IOException {
		metaData.put("parent", newParentId);
		rename(newName);
	}

	public void setContent(byte[] data, String contentType) throws IOException {
		metaData.put("contentType", contentType);
		gridFS.updateContents(metaData, data);
	}

	public Date getModifiedDate() {
		return (Date) metaData.get("updated_at");
	}

	public Date getCreateDate() {
		return (Date) metaData.get("created_at");
	}

	public void delete() {
		gridFS.delete(metaData);
	}

}
