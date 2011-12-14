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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class V7GridFS {

	private final String bucket = "v7files";

	private final DBCollection files;

	private final GridFS fs;

	public V7GridFS(DB db) {
		files = db.getCollection(bucket);
		fs = new GridFS(db, "v7.fs");
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
						parentFile = new V7File(this, metaData);
						continue path;
					}
				}
				return null;
			}
		}

		if (metaData == null)
			return null;
		return new V7File(this, metaData);
	}

	GridFSDBFile findContent(byte[] sha) {
		return fs.findOne(new BasicDBObject("_id", sha));
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
		BasicDBObject metaData = new BasicDBObject("filename", filename)
				.append("_version", 1).append("parent", parentFileId).append(
						"created_at", new Date());

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		if (data != null) {
			byte[] sha = DigestUtils.sha(data);

			if (!contentAlreadyExists(sha)) {
				GridFSInputFile file = fs.createFile(data);
				file.setFilename(filename);
				file.put("_id", sha);
				file.save();
			}
			metaData.append("sha", sha).append("length", data.length);

		}

		updateMetaData(metaData);
		return metaData.get("_id");
	}

	public List<V7File> getChildren(Object parentFileId) {
		List<V7File> children = new ArrayList<V7File>();
		for (DBObject child : files.find(new BasicDBObject("parent",
				parentFileId))) {
			children.add(new V7File(this, child));
		}
		return children;
	}

	void updateMetaData(DBObject metaData) throws IOException {
		WriteResult result = files.save(metaData);
		String error = result.getError();
		if (error != null)
			throw new IOException(error);
	}

	public V7File getChild(Object parentFileId, String childName) {
		DBObject child = files
				.findOne(new BasicDBObject("parent", parentFileId).append(
						"filename", childName));
		if (child == null)
			return null;
		return new V7File(this, child);
	}
}
