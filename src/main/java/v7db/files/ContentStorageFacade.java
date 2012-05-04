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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import v7db.files.spi.Content;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentStorage;
import v7db.files.spi.InlineContent;
import v7db.files.spi.ReferenceTracking;
import v7db.files.spi.StoredContent;

/**
 * Facade that combines the ContentStorage and ReferenceTracking interfaces with
 * translating ContentPointers to and from BSON.
 * 
 */

public class ContentStorageFacade {

	private final ContentStorage storage;

	private final ReferenceTracking refTracking;

	public ContentStorageFacade(ContentStorage storage,
			ReferenceTracking refTracking) {
		this.storage = storage;
		this.refTracking = refTracking;
	}

	public ContentPointer getContentPointer(BSONObject metaData) {
		byte[] inline = (byte[]) metaData.get("in");
		if (inline != null) {
			return new InlineContent(inline);
		}
		byte[] sha = (byte[]) metaData.get("sha");
		if (sha == null)
			return null;
		return new StoredContent(sha, BSONUtils.getRequiredLong(metaData,
				"length"));
	}

	public Content getContent(byte[] sha) throws IOException {
		return storage.getContent(sha);
	}

	public Content getContent(ContentPointer contentPointer) throws IOException {
		return storage.getContent(contentPointer);
	}

	private BasicBSONObject makeMetaData(String filename, String contentType,
			ContentPointer content) {
		BasicBSONObject metaData = new BasicBSONObject();

		if (StringUtils.isNotBlank(filename))
			metaData.append("filename", filename);

		if (StringUtils.isNotBlank(contentType))
			metaData.append("contentType", contentType);

		if (content != null) {
			Map<String, Object> file = content.serialize();

			for (Map.Entry<String, Object> e : file.entrySet()) {
				metaData.append(e.getKey(), e.getValue());
			}
		}

		return metaData;
	}

	public BSONObject insertContentsAndBackRefs(byte[] data, int offset,
			int len, Object fileId, String filename, String contentType)
			throws IOException {

		if (data == null) {
			refTracking.updateReferences(fileId);
			return makeMetaData(filename, contentType, null);

		}

		ContentPointer p = storage.storeContent(new ByteArrayInputStream(data,
				offset, len));

		refTracking.updateReferences(fileId, p);

		return makeMetaData(filename, contentType, p);

	}

	public BSONObject insertContentsAndBackRefs(File data, Object fileId,
			String filename, String contentType) throws IOException {

		if (data == null)
			return insertContentsAndBackRefs(null, 0, 0, fileId, filename,
					contentType);

		ContentPointer p = storage.storeContent(new FileInputStream(data));

		refTracking.updateReferences(fileId, p);

		return makeMetaData(filename, contentType, p);

	}

	public BSONObject inlineOrInsertContentsAndBackRefs(int inlineUntil,
			byte[] data, ObjectId fileId, String filename, String contentType)
			throws IOException {

		if (data == null)
			return insertContentsAndBackRefs(null, 0, 0, fileId, filename,
					contentType);

		if (data.length > inlineUntil)
			return insertContentsAndBackRefs(data, 0, data.length, fileId,
					filename, contentType);

		return makeMetaData(filename, contentType, new InlineContent(data));

	}

	public BSONObject inlineOrInsertContentsAndBackRefs(int inlineUntil,
			byte[] data, int offset, int length, Object fileId,
			String filename, String contentType) throws IOException {
		if (data == null || length > inlineUntil)
			return insertContentsAndBackRefs(data, offset, length, fileId,
					filename, contentType);

		refTracking.updateReferences(fileId);

		return makeMetaData(filename, contentType, new InlineContent(data,
				offset, length));

	}

	public BSONObject inlineOrInsertContentsAndBackRefs(int inlineUntil,
			File data, ObjectId fileId, String filename, String contentType)
			throws IOException {
		if (data == null || data.length() > inlineUntil)
			return insertContentsAndBackRefs(data, fileId, filename,
					contentType);

		refTracking.updateReferences(fileId);

		return makeMetaData(filename, contentType, new InlineContent(FileUtils
				.readFileToByteArray(data)));

	}

	public BSONObject updateBackRefs(ContentPointer content, Object fileId,
			String filename, String contentType) throws IOException {
		refTracking.updateReferences(fileId, content);
		return makeMetaData(filename, contentType, content);
	}

}
