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
package v7db.files.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentSHA;
import v7db.files.spi.InlineContent;
import v7db.files.spi.ReferenceTracking;
import v7db.files.spi.StoredContent;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * References are stored in a collection, with a document for each owner.
 * 
 * <ul>
 * <li> <code>_id</code>: The ownerId as given by the application.
 * <li> <code>refs</code>: The sha hashes used by the ContentPointers from the
 * last updates in an array.
 * <li> <code>refHistory</code>: The sum of all previous and current entries in
 * <code>ref</code>.
 * <li><code>purge</code>: This field is added when the object is to be purged.
 * It contains the timestamp of that event.
 * </ul>
 * 
 * 
 */

public class MongoReferenceTracking implements ReferenceTracking {

	private final DBCollection refCollection;

	public final static String DEFAULT_REFERENCE_COLLECTION_NAME = "v7files.refs";

	public MongoReferenceTracking(DB db) {
		this(db.getCollection(DEFAULT_REFERENCE_COLLECTION_NAME));
	}

	public MongoReferenceTracking(DBCollection refCollection) {
		this.refCollection = refCollection;
	}

	public void purge(Object ownerId) throws IOException {
		refCollection.update(new BasicDBObject("_id", ownerId),
				new BasicDBObject("$set",
						new BasicDBObject("purge", new Date())));
	}

	public void updateReferences(Object ownerId, ContentPointer... contents)
			throws IOException {
		List<byte[]> content = new ArrayList<byte[]>();
		for (ContentPointer cp : contents) {
			if (cp instanceof InlineContent)
				continue;
			if (cp instanceof StoredContent)
				content.add(((StoredContent) cp).getBaseSHA());
			else if (cp instanceof ContentSHA)
				content.add(((ContentSHA) cp).getSHA());
			else
				throw new IllegalArgumentException(cp.getClass().getName());
		}

		WriteResult r = refCollection.update(new BasicDBObject("_id", ownerId),
				new BasicDBObject("$set", new BasicDBObject("refs", content))
						.append("$addToSet", new BasicDBObject("refHistory",
								new BasicDBObject("$each", content))), false,
				false, WriteConcern.SAFE);
		if (r.getN() == 1)
			return;
		if (r.getN() != 0)
			throw new IllegalStateException();
		refCollection.insert(WriteConcern.SAFE, new BasicDBObject("_id",
				ownerId).append("refs", content).append("refHistory", content));

	}

}
