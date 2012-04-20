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
package v7db.files.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;

import v7db.files.BSONUtils;
import v7db.files.MapUtils;

public class ContentConcatenation implements StorageScheme {

	public Content getContent(ContentStorage storage, Map<String, Object> data) {
		List<Content> chunks = new ArrayList<Content>();
		for (Object chunk : MapUtils.values(data, "base")) {
			if (chunk instanceof byte[]) {
				chunks.add(new InlineContent((byte[]) chunk));
			} else if (chunk instanceof BSONObject) {
				BSONObject ch = (BSONObject) chunk;
				long len = BSONUtils.getRequiredLong(ch, "length");
				byte[] sha = (byte[]) ch.get("sha");
				chunks.add(new LazyLoadedContent(storage, new StoredContent(
						sha, len)));
			} else {
				throw new UnsupportedOperationException("chunk " + chunk);
			}
		}
		return new ChunkedContent(chunks.toArray(new Content[0]));
	}

	public String getId() {
		return "cat";
	}

}
