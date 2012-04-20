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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.BasicBSONObject;

import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentStorage;
import v7db.files.spi.ReferenceTracking;
import v7db.files.spi.StoredContent;

import com.mongodb.DBCollection;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoReferenceTrackingTest extends MockMongoTestCaseSupport {

	public void testInsert() throws MongoException, IOException {

		Mongo mongo = new Mongo();

		ReferenceTracking refs = new MongoReferenceTracking(mongo.getDB("test")
				.getCollection("v7files.refs"));

		Object owner = new DBRef(null, "test", "test");

		refs.updateReferences(owner, new StoredContent(new byte[20], 1000));

		assertMockMongoFieldContains(new byte[20], "test.v7files.refs", owner,
				"refs");
		assertMockMongoFieldContains(new byte[20], "test.v7files.refs", owner,
				"refHistory");

		mongo.close();
	}

	public void testUpdate() throws MongoException, IOException {

		byte[] oldRef = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
				14, 15, 16, 17, 18, 19, 20 };

		prepareMockData("test.v7files.refs", new BasicBSONObject("_id", "x")
				.append("refs", new Object[] { oldRef }).append("refHistory",
						new Object[] { oldRef }));

		Mongo mongo = new Mongo();

		ReferenceTracking refs = new MongoReferenceTracking(mongo.getDB("test")
				.getCollection("v7files.refs"));

		refs.updateReferences("x", new StoredContent(new byte[20], 1000));

		assertMockMongoFieldContains(new byte[20], "test.v7files.refs", "x",
				"refs");
		assertMockMongoFieldContains(new byte[20], "test.v7files.refs", "x",
				"refHistory");
		assertMockMongoFieldDoesNotContain(oldRef, "test.v7files.refs", "x",
				"refs");
		assertMockMongoFieldContains(oldRef, "test.v7files.refs", "x",
				"refHistory");

		mongo.close();
	}

	public void testPurge() throws MongoException, IOException,
			DecoderException {
		Mongo mongo = new Mongo();

		DBCollection contents = mongo.getDB("test").getCollection(
				"v7files.content");
		ContentStorage storage = new MongoContentStorage(contents);

		byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();

		ContentPointer pointer = storage.storeContent(new ByteArrayInputStream(
				data));

		DBCollection references = mongo.getDB("test").getCollection(
				"v7files.refs");
		ReferenceTracking refs = new MongoReferenceTracking(references);

		Object owner = new DBRef(null, "test", "test");

		refs.updateReferences(owner, pointer);

		refs.purge(owner);

		SimpleGarbageCollector.purge(contents, references);

		assertMockMongoDoesNotContainDocument("test.v7files.refs", owner);
		assertMockMongoDoesNotContainDocument("test.v7files.content",
				DigestUtils.sha(data));

		mongo.close();
	}
}
