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

	V7File(V7GridFS gridFS, Object fileId) {
		this(gridFS, new BasicDBObject("_id", fileId));
	}

	private void loadGridFile() {
		if (gridFile == null)
			gridFile = gridFS.findContent(getSha());
	}

	// else if ( key.equals( "_id" ) )
	// return _id;
	// else if ( key.equals( "filename" ) )
	// return _filename;
	// else if ( key.equals( "contentType" ) )
	// return _contentType;
	// else if ( key.equals( "length" ) )
	// return _length;
	// else if ( key.equals( "chunkSize" ) )
	// return _chunkSize;
	// else if ( key.equals( "uploadDate" ) )
	// return _uploadDate;
	// else if ( key.equals( "md5" ) )
	// return _md5;
	// return _extradata.get( key );

	public String getContentType() {
		Object x = metaData.get("contentType");
		if (x instanceof String)
			return (String) x;
		return null;
	}

	Object getId() {
		return metaData.get("_id");
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

	public V7File createChild(byte[] data, String filename) throws IOException {
		Object childId = gridFS.addFile(data, getId(), filename);
		return new V7File(gridFS, childId);
	}

	public void rename(String newName) throws IOException {
		metaData.put("filename", newName);
		gridFS.updateMetaData(metaData);
	}
}
