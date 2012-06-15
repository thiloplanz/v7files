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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import v7db.files.spi.Content;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentSHA;
import v7db.files.spi.ContentStorage;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoContentStorageTest extends MockMongoTestCaseSupport {

	public void testRoundtrip() throws MongoException, IOException {

		Mongo mongo = getMongo();
		ContentStorage storage = new MongoContentStorage(mongo.getDB("test")
				.getCollection("v7files.content"));

		byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();

		ContentPointer pointer = storage.storeContent(new ByteArrayInputStream(
				data));
		Content check = storage.getContent(pointer);

		assertEquals(new String(data), IOUtils.toString(check.getInputStream()));
		assertEquals(data.length, check.getLength());
		mongo.close();

	}

	public void testReadCompressedData() throws MongoException, IOException {

		byte[] data = "some data we are going to store compressed with gzip"
				.getBytes();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(baos);
		gzip.write(data);
		gzip.close();
		byte[] compressed = baos.toByteArray();
		byte[] sha = DigestUtils.sha(data);

		prepareMockData("test.v7files.content", new BasicBSONObject("_id", sha)
				.append("store", "gz").append("zin", compressed));

		Mongo mongo = getMongo();
		ContentStorage storage = new MongoContentStorage(mongo.getDB("test")
				.getCollection("v7files.content"));

		Content check = storage.getContent(sha);

		assertEquals(new String(data), IOUtils.toString(check.getInputStream()));
		assertEquals(data.length, check.getLength());
		mongo.close();

	}

	public void testReadChunkedData() throws MongoException, IOException {

		byte[] data1 = "first chunk".getBytes();
		byte[] sha1 = DigestUtils.sha(data1);
		byte[] data2 = " second chunk".getBytes();
		byte[] sha2 = DigestUtils.sha(data2);
		byte[] data = "first chunk second chunk".getBytes();
		byte[] sha = DigestUtils.sha(data);

		prepareMockData("test.v7files.content",
				new BasicBSONObject("_id", sha1).append("in", data1));
		prepareMockData("test.v7files.content",
				new BasicBSONObject("_id", sha2).append("in", data2));
		prepareMockData("test.v7files.content", new BasicBSONObject("_id", sha)
				.append("store", "cat").append(
						"base",
						Arrays.asList(new BasicBSONObject("sha", sha1).append(
								"length", data1.length), new BasicBSONObject(
								"sha", sha2).append("length", data2.length))));

		Mongo mongo = getMongo();
		ContentStorage storage = new MongoContentStorage(mongo.getDB("test")
				.getCollection("v7files.content"));

		Content check = storage.getContent(sha);

		assertEquals(new String(data), IOUtils.toString(check.getInputStream()));
		assertEquals(data.length, check.getLength());
		mongo.close();

	}

	public void testFindByPrefix() throws IOException {
		byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
		byte[] sha = DigestUtils.sha(data);

		prepareMockData("test.v7files.content", new BasicBSONObject("_id", sha)
				.append("in", data));

		Mongo mongo = getMongo();
		MongoContentStorage storage = new MongoContentStorage(mongo
				.getDB("test"));

		assertEquals(new String(data), IOUtils.toString(storage
				.findContentByPrefix(sha).getInputStream()));

		assertEquals(new String(data), IOUtils.toString(storage
				.findContentByPrefix(ArrayUtils.subarray(sha, 0, 5))
				.getInputStream()));

		assertNull(storage.findContentByPrefix(new byte[] { 0, 0, 0, 0 }));
		mongo.close();
	}

	public void testSaveAsChunks() throws IOException {

		byte[] data = new byte[10 * 1024 * 1024];
		new Random(12345).nextBytes(data);
		byte[] sha = DigestUtils.sha(data);

		Mongo mongo = getMongo();
		MongoContentStorage storage = new MongoContentStorage(mongo
				.getDB("test"));

		ContentPointer pointer = storage.storeContent(new ByteArrayInputStream(
				data));

		BSONObject doc = assertMockMongoContainsDocument(
				"test.v7files.content", sha);
		assertEquals("cat", doc.get("store"));
		assertEquals(data.length, storage.getContent(pointer).getLength());
		assertEquals(Hex.encodeHexString(sha), DigestUtils.shaHex(storage
				.getContent(pointer).getInputStream()));

		ContentSHA storeAgain = storage.storeContent(new ByteArrayInputStream(
				data));
		assertEquals(Hex.encodeHexString(sha), storeAgain.getDigest());

	}

}
